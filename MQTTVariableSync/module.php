<?php

declare(strict_types=1);

// IP-Symcon defines a set of instance status constants within the runtime.
// When linting or executing the module in a plain PHP environment (e.g. CI)
// these constants might be missing, which would otherwise cause fatal errors
// once accessed. Provide sensible fallbacks so that development tooling can
// execute the file without requiring the full IP-Symcon context.
if (!defined('IS_INVALIDCONFIG')) {
    define('IS_INVALIDCONFIG', 201);
}

/**
 * MQTTVariableSync Module.
 *
 * Synchronises IP-Symcon variables and their profiles between two installations
 * using MQTT.
 */
class MQTTVariableSync extends IPSModule
{
    private const GUID_MQTT_SERVER = '{C6D2AEB3-6E1F-4B2E-8E69-3A1A00246850}';
    private const GUID_MQTT_CLIENT = '{F7A0DD2E-7684-95C0-64C2-D2A9DC47577B}';
    private const DATA_GUID = '{043EA491-0325-4ADD-8FC2-A30C8EEB4D3F}';

    private const BUFFER_IDENTIFIER_MAP = 'IdentifierMap';
    private const BUFFER_PROFILE_CACHE = 'ProfileCache';
    private const BUFFER_PENDING_ACTIONS = 'PendingActions';
    private const BUFFER_PROCESSING = 'Processing';
    private const BUFFER_KNOWN_PROFILES = 'KnownProfiles';

    private const MESSAGE_VARIABLE_UPDATE = 'variableUpdate';
    private const MESSAGE_SET_VALUE = 'setValue';
    private const MESSAGE_PROFILE = 'profile';
    private const MESSAGE_ACTION_RESULT = 'actionResult';

    private const PROCESSING_REASON_INCOMING = 'incoming';

    /** @var array<int, array{token:string, reason:string}> */
    private array $processing = [];

    /** @inheritDoc */
    public function Create(): void
    {
        parent::Create();
        $this->RegisterPropertyString('Mode', 'Client');
        $this->RegisterPropertyString('BrokerHost', '');
        $this->RegisterPropertyInteger('BrokerPort', 1883);
        $this->RegisterPropertyString('Username', '');
        $this->RegisterPropertyString('Password', '');
        $this->RegisterPropertyString('TopicPrefix', 'ips/sync');
        $this->RegisterPropertyString('SendTopic', 'ips/sync/out');
        $this->RegisterPropertyString('ReceiveTopic', 'ips/sync/in');
        $this->RegisterPropertyString('ProfilePrefix', 'MQTT_');
        $this->RegisterPropertyBoolean('EnableDebug', true);
        $this->RegisterPropertyBoolean('EnableActionLog', true);
        $this->RegisterPropertyInteger('MirrorRoot', 0);
        $this->RegisterPropertyString('SyncTargets', '[]');

        // Use a short-lived timer that kicks off the initial synchronisation right
        // after ApplyChanges() finished wiring up the module. The timer is disabled
        // immediately after the first run.
        $this->RegisterTimer('InitialSync', 0, 'MQTTSync_InitialSync($_IPS["TARGET"]);');

        // Persist all runtime caches as attributes so module reboots do not lose
        // state such as identifier mappings or pending confirmation tokens.
        $this->RegisterAttributeString(self::BUFFER_IDENTIFIER_MAP, json_encode([]));
        $this->RegisterAttributeString(self::BUFFER_PROFILE_CACHE, json_encode([]));
        $this->RegisterAttributeString(self::BUFFER_PENDING_ACTIONS, json_encode([]));
        $this->RegisterAttributeString(self::BUFFER_PROCESSING, json_encode([]));
        $this->RegisterAttributeString(self::BUFFER_KNOWN_PROFILES, json_encode([]));
    }

    /** @inheritDoc */
    public function ApplyChanges(): void
    {
        parent::ApplyChanges();

        // Always reset all subscriptions and references first so we do not retain
        // dangling listeners for previously selected categories when the
        // configuration changes.
        $this->UnregisterMessages();
        $this->UnregisterReferences();

        // Depending on the chosen mode the module needs to connect to the
        // matching MQTT parent instance. The helper takes care of compatibility
        // checks and visual error reporting inside the instance list.
        $this->RegisterParentConnection();
        $this->ConfigureParentInstance();

        // Only accept MQTT messages for the configured receive topic to prevent
        // cross talk between multiple synchronisation pairs.
        $this->MaintainReceiveDataFilter();

        // Observe every selected variable for change notifications so we can
        // emit updates without polling.
        $this->UpdateSynchronizationReferences();

        // The timer is used as a delayed trigger so ApplyChanges() can finish
        // without blocking the PHP thread. The actual synchronisation happens in
        // InitialSync().
        $this->SetTimerInterval('InitialSync', 5000);
    }

    /**
     * Triggered by the timer to push an initial synchronisation after ApplyChanges.
     */
    public function InitialSync(): void
    {
        $this->SetTimerInterval('InitialSync', 0);
        $this->SendDebugExtended('InitialSync', 'Starting initial synchronization');
        $this->PerformFullSync();
    }

    /**
     * Allows manual triggering of a full synchronisation from the UI.
     */
    public function TriggerFullSync(): void
    {
        $this->SendDebugExtended('TriggerFullSync', 'Manual synchronization requested');
        $this->PerformFullSync();
    }

    /**
     * Handles incoming MQTT payloads.
     *
     * @param string $JSONString Encoded MQTT data frame
     */
    public function ReceiveData($JSONString)
    {
        // MQTTClient delivers the payload wrapped in a JSON envelope. Decode it
        // and validate the structure before inspecting the actual message data.
        $data = json_decode($JSONString, true);
        if (!is_array($data)) {
            $this->SendDebugExtended('ReceiveData', 'Invalid data received', $JSONString);
            return;
        }

        $topic = $data['Topic'] ?? '';
        $payload = $data['Payload'] ?? '';

        if ($topic !== $this->ReadPropertyString('ReceiveTopic')) {
            return;
        }

        // The MQTT payload itself is another JSON blob containing the
        // synchronisation metadata and actual value information.
        $decodedPayload = json_decode($payload, true);
        if (!is_array($decodedPayload)) {
            $this->SendDebugExtended('ReceiveData', 'Payload not JSON decodable', $payload);
            return;
        }

        $this->SendDebugExtended('ReceiveData', 'Incoming payload', $decodedPayload);

        $type = $decodedPayload['type'] ?? '';
        switch ($type) {
            case self::MESSAGE_PROFILE:
                $this->HandleProfileSync($decodedPayload);
                break;
            case self::MESSAGE_VARIABLE_UPDATE:
                $this->HandleVariableUpdate($decodedPayload);
                break;
            case self::MESSAGE_SET_VALUE:
                $this->HandleRemoteSetValue($decodedPayload);
                break;
            case self::MESSAGE_ACTION_RESULT:
                $this->HandleActionResult($decodedPayload);
                break;
            default:
                $this->SendDebugExtended('ReceiveData', 'Unknown message type', $type);
        }
    }

    /** @inheritDoc */
    public function MessageSink($TimeStamp, $SenderID, $Message, $Data): void
    {
        if ($Message !== VM_UPDATE) {
            return;
        }

        // When we process a remote update we temporarily mark the variable as
        // busy. This prevents feedback loops because the originating change would
        // otherwise be seen as a new local update.
        if ($this->IsProcessing($SenderID)) {
            $this->SendDebugExtended('MessageSink', sprintf('Skip variable %s because it is currently processed', $SenderID));
            $this->ClearProcessing($SenderID);
            return;
        }

        $this->SendDebugExtended('MessageSink', sprintf('Variable %s changed, syncing', $SenderID), $Data);
        $this->SyncVariable($SenderID);
    }

    /**
     * Handles IPS RequestAction calls for mirrored variables.
     *
     * The method packages the request into an MQTT message that is pushed to the
     * remote peer. The identifier is resolved from the ident that IP-Symcon
     * provides and used as a correlation key for the response topic.
     *
     * @param string $Ident Symbolic ident assigned to the mirror variable.
     * @param mixed  $Value Value to be sent to the remote instance.
     *
     * @return bool True if the request could be transmitted to the remote side.
     */
    public function RequestAction($Ident, $Value): bool
    {
        // Mirror variables are addressed by their identifier which we store in
        // the IP-Symcon ident field. Convert the ident back to the original
        // remote identifier before continuing.
        $identifier = $this->GetIdentifierFromIdent($Ident);
        if ($identifier === null) {
            throw new Exception(sprintf('Unknown ident "%s"', $Ident));
        }

        // Retrieve the variable ID that belongs to the identifier. Without the
        // mapping we cannot resolve which local variable triggered the action.
        $map = $this->GetIdentifierMap();
        if (!isset($map[$identifier])) {
            throw new Exception('Identifier not mapped');
        }

        $variableID = (int) $map[$identifier];
        $this->SendDebugExtended('RequestAction', sprintf('Sending remote set request for variable %s', $variableID), $Value);

        $this->MarkPendingAction($identifier);

        // Serialize the payload so type information survives the MQTT transport
        // and the remote side knows how to convert the value back.
        $message = [
            'type'        => self::MESSAGE_SET_VALUE,
            'identifier'  => $identifier,
            'value'       => $this->SerializeValueForTransmission($Value),
            'valueType'   => IPS_GetVariable($variableID)['VariableType'],
            'timestamp'   => time()
        ];

        $this->Publish($message);

        return true;
    }

    /**
     * Performs a full synchronisation for all configured targets.
     *
     * Every configured category/device tree is traversed recursively and all
     * variables are emitted as MQTT updates. This method is used by the initial
     * synchronisation timer and the manual trigger in the configuration form.
     */
    private function PerformFullSync(): void
    {
        // Gather every eligible variable beforehand so we can process them in a
        // deterministic order and provide consistent debug output.
        $variables = $this->CollectSynchronizationVariables();
        foreach ($variables as $variableID) {
            $this->SyncVariable($variableID, true);
        }
    }

    /**
     * Synchronises a variable to the remote side.
     *
     * The method mirrors profile information, ensures the identifier map is up
     * to date and publishes the value as an MQTT payload. During initial
     * synchronisation it sets the `initial` flag so the receiver can decide
     * whether to overwrite local changes.
     *
     * @param int  $variableID  Identifier of the variable to sync.
     * @param bool $initialSync Flag indicating whether this sync is part of the
     *                          initial bulk sync.
     */
    private function SyncVariable(int $variableID, bool $initialSync = false): void
    {
        if (!IPS_VariableExists($variableID)) {
            $this->SendDebugExtended('SyncVariable', sprintf('Variable %s does not exist anymore', $variableID));
            return;
        }

        $variable = IPS_GetVariable($variableID);
        $value = GetValue($variableID);
        // The identifier contains the full object path and variable ID. It is
        // stable across reboots and allows the partner system to reconstruct the
        // hierarchy faithfully.
        $identifier = $this->BuildIdentifier($variableID);

        $map = $this->GetIdentifierMap();
        if (!isset($map[$identifier]) || (int) $map[$identifier] !== $variableID) {
            $map[$identifier] = $variableID;
            $this->SetIdentifierMap($map);
        }

        $path = $this->BuildObjectPath($variableID);

        $profile = $this->GetProfileInformation($variable);
        if ($profile !== null) {
            $this->PublishProfile($profile);
        }

        $definition = [
            'name'        => $this->GetObjectName($variableID),
            'type'        => $variable['VariableType'],
            'profile'     => $profile['Name'] ?? null,
            'action'      => $variable['VariableCustomAction'] ?: $variable['VariableAction'],
            'ident'       => $identifier
        ];

        $message = [
            'type'         => self::MESSAGE_VARIABLE_UPDATE,
            'identifier'   => $identifier,
            'path'         => $path,
            'definition'   => $definition,
            'value'        => $this->SerializeValueForTransmission($value),
            'valueType'    => $variable['VariableType'],
            'timestamp'    => time(),
            'initial'      => $initialSync
        ];

        $this->SendDebugExtended('SyncVariable', 'Publishing variable update', $message);
        $this->Publish($message);
    }

    /**
     * Publishes a profile definition.
     *
     * Each profile is only sent once per runtime to minimise MQTT traffic. The
     * payload contains the full profile definition so the receiver can recreate
     * the profile with a module specific prefix.
     *
     * @param array $profile Normalised profile array as returned by
     *                       {@see GetProfileInformation()}.
     */
    private function PublishProfile(array $profile): void
    {
        $knownProfiles = $this->GetKnownProfiles();
        if (in_array($profile['Name'], $knownProfiles, true)) {
            return;
        }

        $message = [
            'type'      => self::MESSAGE_PROFILE,
            'profile'   => $profile,
            'timestamp' => time()
        ];

        $this->SendDebugExtended('PublishProfile', 'Publishing profile', $message);
        $this->Publish($message);

        $knownProfiles[] = $profile['Name'];
        $this->SetKnownProfiles($knownProfiles);
    }

    /**
     * Processes an incoming profile synchronisation request.
     *
     * The receiver validates the payload and creates the profile locally if it
     * does not already exist with the configured prefix. Subsequent requests are
     * ignored once the profile cache marks the profile as available.
     *
     * @param array $payload MQTT message payload for a profile update.
     */
    private function HandleProfileSync(array $payload): void
    {
        if (!isset($payload['profile']) || !is_array($payload['profile'])) {
            $this->SendDebugExtended('HandleProfileSync', 'Invalid profile payload', $payload);
            return;
        }

        // Caching avoids recreating profiles on every update and prevents the
        // log from being flooded with duplicate creation attempts.
        $profile = $payload['profile'];
        if (!isset($profile['Name'], $profile['Type'])) {
            $this->SendDebugExtended('HandleProfileSync', 'Incomplete profile data', $payload);
            return;
        }

        $this->EnsureProfileExists($profile);
    }

    /**
     * Processes an incoming variable update.
     *
     * The method ensures the local mirror exists (creating the structure if
     * necessary) and then applies the transmitted value. Loop protection is
     * handled by marking the variable as "processing" before the value is
     * written to IP-Symcon.
     *
     * @param array $payload MQTT message payload describing the variable.
     */
    private function HandleVariableUpdate(array $payload): void
    {
        $identifier = $payload['identifier'] ?? null;
        if (!is_string($identifier) || $identifier === '') {
            $this->SendDebugExtended('HandleVariableUpdate', 'Missing identifier', $payload);
            return;
        }

        $definition = $payload['definition'] ?? null;
        if (!is_array($definition)) {
            $this->SendDebugExtended('HandleVariableUpdate', 'Missing definition', $payload);
            return;
        }

        // Reuse existing mirrors whenever possible to avoid tearing down and
        // recreating the variable on every update.
        $map = $this->GetIdentifierMap();
        $variableID = null;
        if (isset($map[$identifier]) && IPS_VariableExists((int) $map[$identifier])) {
            $variableID = (int) $map[$identifier];
        }

        if ($variableID === null) {
            $variableID = $this->CreateMirrorVariable($identifier, $payload);
            if ($variableID === null) {
                $this->SendDebugExtended('HandleVariableUpdate', 'Failed to create mirror variable', $identifier);
                return;
            }
            $map[$identifier] = $variableID;
            $this->SetIdentifierMap($map);
        }

        if ($this->IsProcessing($variableID)) {
            $this->SendDebugExtended('HandleVariableUpdate', sprintf('Variable %s currently processed, skipping update', $variableID));
            return;
        }

        // Convert the JSON payload back to the appropriate PHP type before the
        // value is written to the local mirror.
        $value = $this->DeserializeValueFromTransmission($payload['value'] ?? null, (int) ($payload['valueType'] ?? 0));

        $this->EnterProcessing($variableID, self::PROCESSING_REASON_INCOMING);
        $this->SendDebugExtended('HandleVariableUpdate', sprintf('Updating mirror variable %s', $variableID), $value);
        SetValue($variableID, $value);
    }

    /**
     * Processes a request to set a variable coming from the remote side.
     *
     * The payload is translated into a local `RequestAction` call on the real
     * variable. Any thrown exception is caught and communicated back via an
     * `actionResult` message so the sender can log the outcome.
     *
     * @param array $payload MQTT message payload containing the target
     *                       identifier and value.
     */
    private function HandleRemoteSetValue(array $payload): void
    {
        $identifier = $payload['identifier'] ?? null;
        $valueType = (int) ($payload['valueType'] ?? 0);
        if (!is_string($identifier)) {
            $this->SendDebugExtended('HandleRemoteSetValue', 'Identifier missing', $payload);
            return;
        }

        // Only attempt to forward the action when the identifier is known.
        // Otherwise we risk calling RequestAction on unrelated variables.
        $map = $this->GetIdentifierMap();
        if (!isset($map[$identifier])) {
            $this->SendDebugExtended('HandleRemoteSetValue', 'Identifier not mapped', $identifier);
            return;
        }

        $variableID = (int) $map[$identifier];
        if (!IPS_VariableExists($variableID)) {
            $this->SendDebugExtended('HandleRemoteSetValue', 'Variable no longer exists', $variableID);
            return;
        }

        $value = $this->DeserializeValueFromTransmission($payload['value'] ?? null, $valueType);

        $result = true;
        $message = 'OK';
        try {
            $this->SendDebugExtended('HandleRemoteSetValue', sprintf('Calling RequestAction for variable %s', $variableID), $value);
            $result = (bool) @RequestAction($variableID, $value);
        } catch (Throwable $exception) {
            $result = false;
            $message = $exception->getMessage();
            $this->SendDebugExtended('HandleRemoteSetValue', 'RequestAction threw exception', $message);
        }

        $this->Publish([
            'type'       => self::MESSAGE_ACTION_RESULT,
            'identifier' => $identifier,
            'success'    => $result,
            'message'    => $message,
            'timestamp'  => time()
        ]);

        if ($this->ReadPropertyBoolean('EnableActionLog')) {
            // Mirror the outcome to the Symcon log for audit trails if the user
            // explicitly requested action logging.
            IPS_LogMessage('MQTTVariableSync', sprintf('Action result for %s: %s (%s)', $identifier, $result ? 'success' : 'failed', $message));
        }
    }

    /**
     * Handles an action result from the remote side.
     *
     * The acknowledgement clears the local pending action marker. When action
     * logging is enabled, the detailed message is forwarded to the Symcon log.
     *
     * @param array $payload MQTT message payload containing the result status.
     */
    private function HandleActionResult(array $payload): void
    {
        $identifier = $payload['identifier'] ?? null;
        if (!is_string($identifier)) {
            return;
        }

        // The identifier acts as correlation token. Remove it from the pending
        // list so the next set request can reuse the slot.
        $pending = $this->GetPendingActions();
        if (isset($pending[$identifier])) {
            unset($pending[$identifier]);
            $this->SetPendingActions($pending);
        }

        if (!$this->ReadPropertyBoolean('EnableActionLog')) {
            return;
        }

        $success = (bool) ($payload['success'] ?? false);
        $message = $payload['message'] ?? '';
        IPS_LogMessage('MQTTVariableSync', sprintf('Remote action for %s %s: %s', $identifier, $success ? 'succeeded' : 'failed', $message));
    }

    /**
     * Creates or fetches a mirrored variable for the provided identifier.
     *
     * The structure defined in the payload is recreated under the configured
     * mirror root and the variable is equipped with the appropriate ident and
     * profile. Afterwards the variable is registered for VM_UPDATE messages so
     * local changes can be forwarded.
     *
     * @param string $identifier Unique identifier produced by
     *                           {@see BuildIdentifier()} on the source side.
     * @param array  $payload    Payload of the variable update message.
     *
     * @return int|null The ID of the mirrored variable or null on error.
     */
    private function CreateMirrorVariable(string $identifier, array $payload): ?int
    {
        $definition = $payload['definition'] ?? [];
        $path = $payload['path'] ?? [];

        // Build the remote category/device tree locally so the structure mirrors
        // the source installation.
        $parentID = $this->EnsureMirrorStructure($path);
        if ($parentID === null) {
            return null;
        }

        $type = (int) ($definition['type'] ?? 0);
        $variableID = IPS_CreateVariable($type);
        IPS_SetParent($variableID, $parentID);
        IPS_SetName($variableID, $definition['name'] ?? 'MQTT Variable');

        $ident = $this->BuildIdentFromIdentifier($identifier);
        IPS_SetIdent($variableID, $ident);
        IPS_SetVariableCustomAction($variableID, $this->InstanceID);

        $profileName = $definition['profile'] ?? '';
        if ($profileName !== '') {
            // Ensure the prefixed profile exists before assigning it to the
            // mirror variable.
            $targetProfile = $this->EnsureProfileExistsByName($profileName, $type);
            if ($targetProfile !== null) {
                IPS_SetVariableCustomProfile($variableID, $targetProfile);
            }
        }

        $this->SendDebugExtended('CreateMirrorVariable', sprintf('Created mirror variable %s for identifier %s', $variableID, $identifier));

        $this->RegisterMessage($variableID, VM_UPDATE);
        $this->RegisterReference($variableID);

        return $variableID;
    }

    /**
     * Ensures the mirror structure exists for the provided path.
     *
     * The path represents the object hierarchy on the remote system. For each
     * node a category is created locally (if not already present) underneath the
     * configured mirror root. Links are ignored as they could otherwise create
     * recursive structures.
     *
     * @param array $path Structured path information from the remote instance.
     *
     * @return int|null ID of the parent object that should contain the variable
     *                  or null if the structure could not be ensured.
     */
    private function EnsureMirrorStructure(array $path): ?int
    {
        $root = $this->ReadPropertyInteger('MirrorRoot');
        if ($root <= 0 || !IPS_ObjectExists($root)) {
            $this->SendDebugExtended('EnsureMirrorStructure', 'Mirror root invalid', $root);
            return null;
        }

        $parentID = $root;
        foreach ($path as $node) {
            $name = $node['name'] ?? 'MQTT';
            $identifier = $node['identifier'] ?? md5($name);
            $ident = $this->BuildIdentFromIdentifier((string) $identifier);

            $childID = @IPS_GetObjectIDByIdent($ident, $parentID);
            if ($childID === false) {
                // Create missing hierarchy segments to maintain the structural
                // parity between both installations.
                $childID = IPS_CreateCategory();
                IPS_SetName($childID, $name);
                IPS_SetIdent($childID, $ident);
                IPS_SetParent($childID, $parentID);
            }
            $parentID = $childID;
        }

        return $parentID;
    }

    /**
     * Ensures a profile exists locally, creating it if necessary.
     *
     * The method creates a prefixed copy of the original profile when it is not
     * already available. All numeric ranges, digits, texts and associations are
     * mirrored so the user experience stays the same on the mirrored instance.
     *
     * @param array $profile Raw profile definition supplied by the remote side.
     */
    private function EnsureProfileExists(array $profile): void
    {
        $profileName = (string) $profile['Name'];
        $profileType = (int) $profile['Type'];

        if (IPS_VariableProfileExists($profileName)) {
            return;
        }

        $prefixedName = $this->BuildPrefixedProfileName($profileName);
        if (!IPS_VariableProfileExists($prefixedName)) {
            // Create a copy of the profile using the module prefix to avoid
            // collisions with existing user profiles.
            IPS_CreateVariableProfile($prefixedName, $profileType);
            $min = $profile['MinValue'] ?? 0;
            $max = $profile['MaxValue'] ?? 0;
            $step = $profile['StepSize'] ?? 0;
            IPS_SetVariableProfileValues($prefixedName, $min, $max, $step);
            IPS_SetVariableProfileDigits($prefixedName, $profile['Digits'] ?? 0);
            IPS_SetVariableProfileText($prefixedName, $profile['Prefix'] ?? '', $profile['Suffix'] ?? '');

            if (isset($profile['Associations']) && is_array($profile['Associations'])) {
                foreach ($profile['Associations'] as $association) {
                    IPS_SetVariableProfileAssociation(
                        $prefixedName,
                        $association['Value'],
                        $association['Name'],
                        $association['Icon'],
                        $association['Color']
                    );
                }
            }
        }

        // Remember the exact definition so later calls can recreate it without
        // waiting for another profile payload.
        $cache = $this->GetProfileCache();
        $cache[$profileName] = $profile;
        $this->SetProfileCache($cache);
    }

    /**
     * Ensures a profile exists locally and returns the usable profile name.
     *
     * When the original profile is not available, the method attempts to use a
     * cached definition to build the prefixed variant. If the cache does not
     * contain the required metadata or the types differ, null is returned.
     *
     * @param string $profileName Original profile name from the source system.
     * @param int    $type        Expected variable type to guard against
     *                            incompatible profiles.
     *
     * @return string|null Name of the profile that should be applied locally.
     */
    private function EnsureProfileExistsByName(string $profileName, int $type): ?string
    {
        if (IPS_VariableProfileExists($profileName)) {
            return $profileName;
        }

        $cache = $this->GetProfileCache();
        if (!isset($cache[$profileName])) {
            $this->SendDebugExtended('EnsureProfileExistsByName', 'No profile data available', $profileName);
            return null;
        }

        $profile = $cache[$profileName];
        // Guard against accidentally applying a profile with a mismatching type
        // which could break widget rendering in IP-Symcon.
        if ((int) $profile['Type'] !== $type) {
            $this->SendDebugExtended('EnsureProfileExistsByName', 'Profile type mismatch', $profile);
            return null;
        }

        $this->EnsureProfileExists($profile);
        $prefixedName = $this->BuildPrefixedProfileName($profileName);
        if (IPS_VariableProfileExists($prefixedName)) {
            return $prefixedName;
        }

        return null;
    }

    /**
     * Stores profile information for reuse when receiving variables.
     *
     * The returned structure is cached so later calls to
     * {@see EnsureProfileExistsByName()} can recreate the profile if necessary.
     *
     * @param array $variable Variable data from `IPS_GetVariable`.
     *
     * @return array|null Normalised profile definition or null when none exists.
     */
    private function GetProfileInformation(array $variable): ?array
    {
        $profileName = $variable['VariableCustomProfile'] ?: $variable['VariableProfile'];
        if ($profileName === '') {
            return null;
        }

        if (!IPS_VariableProfileExists($profileName)) {
            return null;
        }

        $profile = IPS_GetVariableProfile($profileName);
        $profile['Name'] = $profileName;

        // Cache the profile immediately so EnsureProfileExistsByName() can rely
        // on the data even if the profile is deleted later on the source side.
        $cache = $this->GetProfileCache();
        $cache[$profileName] = $profile;
        $this->SetProfileCache($cache);

        return $profile;
    }

    /**
     * Publishes a message to the MQTT broker.
     *
     * The message is wrapped in an IP-Symcon specific MQTT data frame and
     * forwarded to the connected client/server instance. Messages are sent as
     * QoS 0 and are not retained to avoid stale data after restarts.
     *
     * @param array $payload Associative array that will be JSON encoded.
     */
    private function Publish(array $payload): void
    {
        $topic = $this->ReadPropertyString('SendTopic');
        $data = [
            'DataID'           => self::DATA_GUID,
            'PacketType'       => 3,
            'QualityOfService' => 0,
            'Retain'           => false,
            'Topic'            => $topic,
            'Payload'          => json_encode($payload)
        ];

        // Provide transparency about the MQTT exchange while keeping message
        // formatting consistent in a single helper.
        $this->SendDebugExtended('Publish', sprintf('Publishing to %s', $topic), $payload);
        $this->SendDataToParent(json_encode($data));
    }

    /**
     * Builds the identifier for a variable.
     *
     * Currently the IP-Symcon location string is used which ensures uniqueness
     * within the instance and is stable across restarts as long as the object
     * path is unchanged.
     *
     * @param int $variableID ID of the variable to build the identifier for.
     *
     * @return string Unique identifier shared with the remote instance.
     */
    private function BuildIdentifier(int $variableID): string
    {
        return IPS_GetLocation($variableID);
    }

    /**
     * Builds a sanitized ident string for mirrored objects.
     *
     * IP-Symcon idents must be alphanumeric and unique per parent. The MD5 hash
     * guarantees uniqueness even for very long identifiers.
     *
     * @param string $identifier Full identifier that was exchanged with the peer.
     *
     * @return string Sanitised ident that can be applied to objects.
     */
    private function BuildIdentFromIdentifier(string $identifier): string
    {
        return 'MQTTSYNC_' . md5($identifier);
    }

    /**
     * Converts an ident back to the stored identifier.
     *
     * The lookup iterates over the identifier map and resolves the ident stored
     * on each object. This makes the function resilient to map inconsistencies
     * after manual object changes.
     *
     * @param string $ident Ident assigned to the mirrored object.
     *
     * @return string|null Original identifier or null if not resolvable.
     */
    private function GetIdentifierFromIdent(string $ident): ?string
    {
        $map = $this->GetIdentifierMap();
        foreach ($map as $identifier => $variableID) {
            if (IPS_VariableExists((int) $variableID)) {
                $storedIdent = IPS_GetObject((int) $variableID)['ObjectIdent'] ?? '';
                if ($storedIdent === $ident) {
                    return $identifier;
                }
            }
        }

        return null;
    }

    /**
     * Builds an object path for a variable to replicate on the remote side.
     *
     * The path consists of all parents (excluding links) up to the root. It is
     * used to recreate the same category/device structure when a mirror
     * variable is created on the peer.
     *
     * @param int $variableID ID of the variable whose path should be exported.
     *
     * @return array<int, array<string, mixed>> Ordered path description.
     */
    private function BuildObjectPath(int $variableID): array
    {
        $path = [];
        $objectID = IPS_GetObject($variableID)['ParentID'];
        while ($objectID > 0) {
            $object = IPS_GetObject($objectID);
            if ($object['ObjectType'] === OBJECTTYPE_LINK) {
                break;
            }
            // Each segment stores name and ident so the receiver can recreate
            // the exact hierarchy beneath the configured mirror root.
            $path[] = [
                'id'         => $objectID,
                'name'       => $object['ObjectName'],
                'identifier' => ($object['ObjectIdent'] !== '') ? $object['ObjectIdent'] : (string) $objectID
            ];
            $objectID = $object['ParentID'];
        }
        $path = array_reverse($path);
        return $path;
    }

    /**
     * Collects all variables from the configured targets.
     *
     * Targets can be provided either as plain IDs or as objects with an
     * `ObjectID` key (matching the configuration form schema). Duplicates are
     * removed before returning the list.
     *
     * @return int[] List of variable IDs that should be synchronised.
     */
    private function CollectSynchronizationVariables(): array
    {
        $targets = json_decode($this->ReadPropertyString('SyncTargets'), true);
        if (!is_array($targets)) {
            return [];
        }

        $variables = [];
        foreach ($targets as $entry) {
            if (is_array($entry) && isset($entry['ObjectID'])) {
                $targetID = (int) $entry['ObjectID'];
            } else {
                $targetID = (int) $entry;
            }
            if ($targetID <= 0 || !IPS_ObjectExists($targetID)) {
                // Skip stale configuration entries gracefully to keep the
                // synchronisation resilient against manual tree changes.
                continue;
            }
            $this->SendDebugExtended('CollectSynchronizationVariables', 'Collecting from target', $targetID);
            $this->CollectVariablesRecursive($targetID, $variables);
        }

        return array_unique($variables);
    }

    /**
     * Recursively collects variables for synchronisation.
     *
     * Links are skipped to avoid cycles. Every variable encountered is appended
     * to the accumulator array.
     *
     * @param int   $objectID  Object to traverse.
     * @param array $variables Reference to the accumulator.
     */
    private function CollectVariablesRecursive(int $objectID, array &$variables): void
    {
        $object = IPS_GetObject($objectID);
        if ($object['ObjectType'] === OBJECTTYPE_LINK) {
            return;
        }

        if ($object['ObjectType'] === OBJECTTYPE_VARIABLE) {
            // Directly append variables; recursion stops here to avoid
            // traversing into non-existing children.
            $variables[] = $objectID;
            return;
        }

        foreach ($object['ChildrenIDs'] as $childID) {
            // Depth-first traversal ensures that nested categories are processed
            // completely before moving on to the next branch.
            $this->CollectVariablesRecursive($childID, $variables);
        }
    }

    /**
     * Registers parent connection based on configuration.
     *
     * Depending on the selected mode the module connects either to an
     * MQTTClient or MQTTServer instance.
     */
    private function RegisterParentConnection(): void
    {
        $mode = $this->ReadPropertyString('Mode');
        $parentGUID = ($mode === 'Server') ? self::GUID_MQTT_SERVER : self::GUID_MQTT_CLIENT;

        // Not every IP-Symcon installation necessarily has the MQTT modules
        // installed. Avoid the rather cryptic warning "Module with GUID ... not
        // found" by checking the availability up front and surface a descriptive
        // status message instead.
        if (function_exists('IPS_ModuleExists') && !IPS_ModuleExists($parentGUID)) {
            $message = sprintf(
                'Das benötigte MQTT-%s Modul (GUID %s) ist nicht installiert. Bitte das offizielle MQTT-Modul aus dem Module-Store hinzufügen oder den Modus wechseln.',
                ($mode === 'Server') ? 'Server' : 'Client',
                $parentGUID
            );
            $this->SendDebug('Parent', $message, 0);
            IPS_LogMessage('MQTTVariableSync', $message);
            $this->SetStatus(IS_INVALIDCONFIG);
            return;
        }

        // ConnectParent() automatically creates the required instance if it does
        // not yet exist, making the setup experience smoother for users.
        $this->ConnectParent($parentGUID);
    }

    /**
     * Applies configuration parameters to the connected parent instance if supported.
     *
     * This allows the module to automatically propagate broker settings such as
     * host, port and credentials to the MQTT instance.
     */
    private function ConfigureParentInstance(): void
    {
        $instance = IPS_GetInstance($this->InstanceID);
        $parentID = $instance['ConnectionID'] ?? 0;
        if ($parentID === 0) {
            return;
        }

        $config = json_decode(IPS_GetConfiguration($parentID), true);
        if (!is_array($config)) {
            return;
        }

        $changed = false;

        $mode = $this->ReadPropertyString('Mode');
        if ($mode === 'Client') {
            $changed = $this->SetParentPropertyIfExists($parentID, $config, 'Host', $this->ReadPropertyString('BrokerHost')) || $changed;
            $changed = $this->SetParentPropertyIfExists($parentID, $config, 'Port', $this->ReadPropertyInteger('BrokerPort')) || $changed;
        } else {
            $changed = $this->SetParentPropertyIfExists($parentID, $config, 'BindPort', $this->ReadPropertyInteger('BrokerPort')) || $changed;
        }

        $changed = $this->SetParentPropertyIfExists($parentID, $config, 'Username', $this->ReadPropertyString('Username')) || $changed;
        $changed = $this->SetParentPropertyIfExists($parentID, $config, 'Password', $this->ReadPropertyString('Password')) || $changed;

        if ($changed) {
            IPS_ApplyChanges($parentID);
        }
    }

    /**
     * Helper to set a property on the parent instance if the configuration supports it.
     *
     * @param int    $parentID ID of the parent instance.
     * @param array  $config   Current configuration snapshot of the instance.
     * @param string $property Property name in the parent configuration.
     * @param mixed  $value    New value to apply.
     *
     * @return bool True when the configuration value was changed.
     */
    private function SetParentPropertyIfExists(int $parentID, array $config, string $property, $value): bool
    {
        if (!array_key_exists($property, $config)) {
            // Some MQTT parent instances may not expose all configuration keys;
            // ignore them silently to remain compatible across versions.
            return false;
        }

        if ($config[$property] === $value) {
            return false;
        }

        IPS_SetProperty($parentID, $property, $value);
        return true;
    }

    /**
     * Updates the receive data filter to only accept configured topics.
     *
     * Restricting the filter minimises processing overhead and prevents other
     * MQTT traffic from triggering the module.
     */
    private function MaintainReceiveDataFilter(): void
    {
        $topic = preg_quote($this->ReadPropertyString('ReceiveTopic'), '/');
        // The filter is a JSON snippet evaluated by IP-Symcon before the module
        // receives the data. It keeps unrelated MQTT traffic away from the
        // instance to reduce processing overhead.
        $filter = sprintf('("Topic"\s*:\s*"%s")', $topic);
        $this->SetReceiveDataFilter($filter);
    }

    /**
     * Updates references and message registrations for configured variables.
     *
     * This ensures VM_UPDATE messages are received for every configured
     * variable and that the mirror root is tracked as a reference.
     */
    private function UpdateSynchronizationReferences(): void
    {
        $root = $this->ReadPropertyInteger('MirrorRoot');
        if ($root > 0 && IPS_ObjectExists($root)) {
            $this->RegisterReference($root);
        }

        $variables = $this->CollectSynchronizationVariables();
        foreach ($variables as $variableID) {
            // Register both message and reference so the IP-Symcon UI displays
            // the relationship and the module receives VM_UPDATE events.
            $this->RegisterReference($variableID);
            $this->RegisterMessage($variableID, VM_UPDATE);
        }
    }

    /**
     * Removes all registered messages.
     *
     * Also clears the processing buffer to avoid stale state between
     * reconfigurations.
     */
    private function UnregisterMessages(): void
    {
        foreach ($this->GetReferenceList() as $referenceID) {
            // Messages are scoped per object, therefore we remove each
            // VM_UPDATE subscription explicitly.
            $this->UnregisterMessage($referenceID, VM_UPDATE);
        }

        $this->processing = [];
        $this->SetProcessing([]);
    }

    /**
     * Removes registered references.
     *
     * Called before rebuilding the reference list during ApplyChanges.
     */
    private function UnregisterReferences(): void
    {
        $references = $this->GetReferenceList();
        foreach ($references as $referenceID) {
            $this->UnregisterReference($referenceID);
        }
    }

    /**
     * Stores that a variable is being processed to avoid loops.
     *
     * @param int    $variableID Variable currently being processed.
     * @param string $reason     Human readable reason (incoming/outgoing).
     */
    private function EnterProcessing(int $variableID, string $reason): void
    {
        $token = uniqid('proc', true);
        $processing = $this->GetProcessing();
        $processing[$variableID] = ['token' => $token, 'reason' => $reason];
        $this->SetProcessing($processing);
        $this->processing[$variableID] = ['token' => $token, 'reason' => $reason];
    }

    /**
     * Clears the processing flag for a variable.
     *
     * @param int $variableID Variable that finished processing.
     */
    private function ClearProcessing(int $variableID): void
    {
        $processing = $this->GetProcessing();
        if (isset($processing[$variableID])) {
            unset($processing[$variableID]);
            $this->SetProcessing($processing);
        }
        unset($this->processing[$variableID]);
    }

    /**
     * Determines whether a variable is currently being processed.
     *
     * @param int $variableID Variable to check.
     *
     * @return bool True when the variable is within the processing buffer.
     */
    private function IsProcessing(int $variableID): bool
    {
        if (isset($this->processing[$variableID])) {
            return true;
        }

        $processing = $this->GetProcessing();
        return isset($processing[$variableID]);
    }

    /**
     * Marks a pending action (remote RequestAction call) for logging.
     *
     * @param string $identifier Identifier of the variable action.
     */
    private function MarkPendingAction(string $identifier): void
    {
        $pending = $this->GetPendingActions();
        // Store a timestamp mainly for debugging purposes so administrators can
        // see how long an action was outstanding before the acknowledgement
        // arrived.
        $pending[$identifier] = time();
        $this->SetPendingActions($pending);
    }

    /**
     * Fetches the identifier map.
     *
     * @return array<string, int> Mapping between identifiers and local IDs.
     */
    private function GetIdentifierMap(): array
    {
        return json_decode($this->ReadAttributeString(self::BUFFER_IDENTIFIER_MAP), true) ?? [];
    }

    /**
     * Stores the identifier map.
     *
     * @param array<string, int> $map Mapping between identifiers and local IDs.
     */
    private function SetIdentifierMap(array $map): void
    {
        $this->WriteAttributeString(self::BUFFER_IDENTIFIER_MAP, json_encode($map));
    }

    /**
     * Fetches cached profiles.
     *
     * @return array<string, array> Profile definitions keyed by name.
     */
    private function GetProfileCache(): array
    {
        return json_decode($this->ReadAttributeString(self::BUFFER_PROFILE_CACHE), true) ?? [];
    }

    /**
     * Stores profile cache.
     *
     * @param array<string, array> $cache Profile definitions keyed by name.
     */
    private function SetProfileCache(array $cache): void
    {
        $this->WriteAttributeString(self::BUFFER_PROFILE_CACHE, json_encode($cache));
    }

    /**
     * Fetches pending actions.
     *
     * @return array<string, int> Pending identifiers with timestamps.
     */
    private function GetPendingActions(): array
    {
        return json_decode($this->ReadAttributeString(self::BUFFER_PENDING_ACTIONS), true) ?? [];
    }

    /**
     * Stores pending actions.
     *
     * @param array<string, int> $pending Pending identifiers with timestamps.
     */
    private function SetPendingActions(array $pending): void
    {
        $this->WriteAttributeString(self::BUFFER_PENDING_ACTIONS, json_encode($pending));
    }

    /**
     * Retrieves processing buffer.
     *
     * @return array<int, array{token:string, reason:string}> Processing metadata keyed by variable ID.
     */
    private function GetProcessing(): array
    {
        return json_decode($this->ReadAttributeString(self::BUFFER_PROCESSING), true) ?? [];
    }

    /**
     * Stores processing buffer.
     *
     * @param array<int, array{token:string, reason:string}> $processing Processing metadata keyed by variable ID.
     */
    private function SetProcessing(array $processing): void
    {
        $this->WriteAttributeString(self::BUFFER_PROCESSING, json_encode($processing));
    }

    /**
     * Retrieves known profiles.
     *
     * @return array<int, string> List of already published profile names.
     */
    private function GetKnownProfiles(): array
    {
        return json_decode($this->ReadAttributeString(self::BUFFER_KNOWN_PROFILES), true) ?? [];
    }

    /**
     * Stores known profiles.
     *
     * @param array<int, string> $profiles List of already published profile names.
     */
    private function SetKnownProfiles(array $profiles): void
    {
        $this->WriteAttributeString(self::BUFFER_KNOWN_PROFILES, json_encode($profiles));
    }

    /**
     * Serialises a value for MQTT transfer.
     *
     * Complex data structures are JSON encoded, primitive types are passed
     * through unchanged.
     *
     * @param mixed $value Value to be sent over MQTT.
     *
     * @return mixed Serialised representation.
     */
    private function SerializeValueForTransmission($value)
    {
        if (is_string($value) || is_bool($value) || is_int($value) || is_float($value) || $value === null) {
            // Primitive types can be transported as-is without loosing
            // information.
            return $value;
        }

        // Complex types (arrays, objects) are encoded into JSON so the receiver
        // can rehydrate them if required by custom logic.
        return json_encode($value);
    }

    /**
     * Deserialises an incoming value according to the variable type.
     *
     * @param mixed $value Raw value from the MQTT payload.
     * @param int   $type  Variable type constant from IP-Symcon.
     *
     * @return mixed Value converted to the expected PHP type.
     */
    private function DeserializeValueFromTransmission($value, int $type)
    {
        switch ($type) {
            case VARIABLETYPE_BOOLEAN:
                return (bool) $value;
            case VARIABLETYPE_INTEGER:
                return (int) $value;
            case VARIABLETYPE_FLOAT:
                return (float) $value;
            case VARIABLETYPE_STRING:
            default:
                // Strings (and complex types encoded as strings) fall through to
                // the default case.
                return (string) $value;
        }
    }

    /**
     * Returns object name including custom name.
     *
     * @param int $objectID Object whose display name should be resolved.
     *
     * @return string Human readable object name.
     */
    private function GetObjectName(int $objectID): string
    {
        $object = IPS_GetObject($objectID);
        // Prefer the object name but fall back to the ident when no custom name
        // was assigned to keep the mirror readable.
        $name = $object['ObjectName'];
        if ($name === '') {
            $name = $object['ObjectIdent'] ?: ('ID' . $objectID);
        }

        return $name;
    }

    /**
     * Builds a prefixed profile name.
     *
     * @param string $profileName Original profile name.
     *
     * @return string Prefixed profile identifier for the local system.
     */
    private function BuildPrefixedProfileName(string $profileName): string
    {
        $prefix = $this->ReadPropertyString('ProfilePrefix');
        // Avoid double-prefixing when the profile already starts with the
        // configured prefix.
        if ($prefix !== '' && strpos($profileName, $prefix) === 0) {
            return $profileName;
        }
        return $prefix . $profileName;
    }

    /**
     * Extended debug helper with optional structured data.
     *
     * @param string     $context Short tag to identify the caller.
     * @param string     $message Log message.
     * @param mixed|null $data    Optional payload that will be JSON encoded.
     */
    private function SendDebugExtended(string $context, string $message, $data = null): void
    {
        if (!$this->ReadPropertyBoolean('EnableDebug')) {
            return;
        }

        if ($data !== null) {
            // Encode complex data as JSON to preserve structure within the debug
            // log while keeping it readable.
            $message .= ' | ' . json_encode($data);
        }
        $this->SendDebug($context, $message, 0);
    }
}

