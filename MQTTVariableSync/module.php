<?php

declare(strict_types=1);

/**
 * MQTTVariableSync Module.
 *
 * Synchronises IP-Symcon variables and their profiles between two installations
 * using MQTT.
 */
class MQTTVariableSync extends IPSModule
{
    private const GUID_MQTT_SERVER = '{C6E74E45-0BC7-4757-84F4-6BAF3BEA1D43}';
    private const GUID_MQTT_CLIENT = '{C6E74E41-0BC7-4757-84F4-6BAF3BEA1D43}';
    private const DATA_GUID = '{F6873AC2-8557-4E4B-AB5A-6961C897A3D2}';

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

        $this->RegisterTimer('InitialSync', 0, 'MQTTSync_InitialSync($_IPS["TARGET"]);');

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

        $this->UnregisterMessages();
        $this->UnregisterReferences();

        $this->RegisterParentConnection();
        $this->ConfigureParentInstance();

        $this->MaintainReceiveDataFilter();

        $this->UpdateSynchronizationReferences();

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
    public function ReceiveData(string $JSONString): void
    {
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
     * @param string $Ident
     * @param mixed  $Value
     */
    public function RequestAction($Ident, $Value): bool
    {
        $identifier = $this->GetIdentifierFromIdent($Ident);
        if ($identifier === null) {
            throw new Exception(sprintf('Unknown ident "%s"', $Ident));
        }

        $map = $this->GetIdentifierMap();
        if (!isset($map[$identifier])) {
            throw new Exception('Identifier not mapped');
        }

        $variableID = (int) $map[$identifier];
        $this->SendDebugExtended('RequestAction', sprintf('Sending remote set request for variable %s', $variableID), $Value);

        $this->MarkPendingAction($identifier);

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
     */
    private function PerformFullSync(): void
    {
        $variables = $this->CollectSynchronizationVariables();
        foreach ($variables as $variableID) {
            $this->SyncVariable($variableID, true);
        }
    }

    /**
     * Synchronises a variable to the remote side.
     *
     * @param int  $variableID   Identifier of the variable to sync.
     * @param bool $initialSync  Flag indicating whether this sync is part of the initial bulk sync.
     */
    private function SyncVariable(int $variableID, bool $initialSync = false): void
    {
        if (!IPS_VariableExists($variableID)) {
            $this->SendDebugExtended('SyncVariable', sprintf('Variable %s does not exist anymore', $variableID));
            return;
        }

        $variable = IPS_GetVariable($variableID);
        $value = GetValue($variableID);
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
     * @param array $profile
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
     * @param array $payload
     */
    private function HandleProfileSync(array $payload): void
    {
        if (!isset($payload['profile']) || !is_array($payload['profile'])) {
            $this->SendDebugExtended('HandleProfileSync', 'Invalid profile payload', $payload);
            return;
        }

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
     * @param array $payload
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

        $value = $this->DeserializeValueFromTransmission($payload['value'] ?? null, (int) ($payload['valueType'] ?? 0));

        $this->EnterProcessing($variableID, self::PROCESSING_REASON_INCOMING);
        $this->SendDebugExtended('HandleVariableUpdate', sprintf('Updating mirror variable %s', $variableID), $value);
        SetValue($variableID, $value);
    }

    /**
     * Processes a request to set a variable coming from the remote side.
     *
     * @param array $payload
     */
    private function HandleRemoteSetValue(array $payload): void
    {
        $identifier = $payload['identifier'] ?? null;
        $valueType = (int) ($payload['valueType'] ?? 0);
        if (!is_string($identifier)) {
            $this->SendDebugExtended('HandleRemoteSetValue', 'Identifier missing', $payload);
            return;
        }

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
            IPS_LogMessage('MQTTVariableSync', sprintf('Action result for %s: %s (%s)', $identifier, $result ? 'success' : 'failed', $message));
        }
    }

    /**
     * Handles an action result from the remote side.
     *
     * @param array $payload
     */
    private function HandleActionResult(array $payload): void
    {
        $identifier = $payload['identifier'] ?? null;
        if (!is_string($identifier)) {
            return;
        }

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
     * @param string $identifier
     * @param array  $payload
     *
     * @return int|null
     */
    private function CreateMirrorVariable(string $identifier, array $payload): ?int
    {
        $definition = $payload['definition'] ?? [];
        $path = $payload['path'] ?? [];

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
     * @param array $path
     *
     * @return int|null
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
     * @param array $profile
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

        $cache = $this->GetProfileCache();
        $cache[$profileName] = $profile;
        $this->SetProfileCache($cache);
    }

    /**
     * Ensures a profile exists locally and returns the usable profile name.
     *
     * @param string $profileName
     * @param int    $type
     *
     * @return string|null
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
     * @param array $variable
     *
     * @return array|null
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

        $cache = $this->GetProfileCache();
        $cache[$profileName] = $profile;
        $this->SetProfileCache($cache);

        return $profile;
    }

    /**
     * Publishes a message to the MQTT broker.
     *
     * @param array $payload
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

        $this->SendDebugExtended('Publish', sprintf('Publishing to %s', $topic), $payload);
        $this->SendDataToParent(json_encode($data));
    }

    /**
     * Builds the identifier for a variable.
     *
     * @param int $variableID
     *
     * @return string
     */
    private function BuildIdentifier(int $variableID): string
    {
        return IPS_GetLocation($variableID);
    }

    /**
     * Builds a sanitized ident string for mirrored objects.
     *
     * @param string $identifier
     *
     * @return string
     */
    private function BuildIdentFromIdentifier(string $identifier): string
    {
        return 'MQTTSYNC_' . md5($identifier);
    }

    /**
     * Converts an ident back to the stored identifier.
     *
     * @param string $ident
     *
     * @return string|null
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
     * @param int $variableID
     *
     * @return array<int, array<string, mixed>>
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
     * @return int[]
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
     * @param int   $objectID
     * @param array $variables
     */
    private function CollectVariablesRecursive(int $objectID, array &$variables): void
    {
        $object = IPS_GetObject($objectID);
        if ($object['ObjectType'] === OBJECTTYPE_LINK) {
            return;
        }

        if ($object['ObjectType'] === OBJECTTYPE_VARIABLE) {
            $variables[] = $objectID;
            return;
        }

        foreach ($object['ChildrenIDs'] as $childID) {
            $this->CollectVariablesRecursive($childID, $variables);
        }
    }

    /**
     * Registers parent connection based on configuration.
     */
    private function RegisterParentConnection(): void
    {
        $mode = $this->ReadPropertyString('Mode');
        $parentGUID = ($mode === 'Server') ? self::GUID_MQTT_SERVER : self::GUID_MQTT_CLIENT;
        $this->ConnectParent($parentGUID);
    }

    /**
     * Applies configuration parameters to the connected parent instance if supported.
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
     * @param int   $parentID
     * @param array $config
     * @param string $property
     * @param mixed $value
     *
     * @return bool
     */
    private function SetParentPropertyIfExists(int $parentID, array $config, string $property, $value): bool
    {
        if (!array_key_exists($property, $config)) {
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
     */
    private function MaintainReceiveDataFilter(): void
    {
        $topic = preg_quote($this->ReadPropertyString('ReceiveTopic'), '/');
        $filter = sprintf('("Topic"\s*:\s*"%s")', $topic);
        $this->SetReceiveDataFilter($filter);
    }

    /**
     * Updates references and message registrations for configured variables.
     */
    private function UpdateSynchronizationReferences(): void
    {
        $root = $this->ReadPropertyInteger('MirrorRoot');
        if ($root > 0 && IPS_ObjectExists($root)) {
            $this->RegisterReference($root);
        }

        $variables = $this->CollectSynchronizationVariables();
        foreach ($variables as $variableID) {
            $this->RegisterReference($variableID);
            $this->RegisterMessage($variableID, VM_UPDATE);
        }
    }

    /**
     * Removes all registered messages.
     */
    private function UnregisterMessages(): void
    {
        foreach ($this->GetReferenceList() as $referenceID) {
            $this->UnregisterMessage($referenceID, VM_UPDATE);
        }

        $this->processing = [];
        $this->SetProcessing([]);
    }

    /**
     * Removes registered references.
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
     * @param int    $variableID
     * @param string $reason
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
     * @param int $variableID
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
     * @param int $variableID
     *
     * @return bool
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
     * @param string $identifier
     */
    private function MarkPendingAction(string $identifier): void
    {
        $pending = $this->GetPendingActions();
        $pending[$identifier] = time();
        $this->SetPendingActions($pending);
    }

    /**
     * Fetches the identifier map.
     *
     * @return array<string, int>
     */
    private function GetIdentifierMap(): array
    {
        return json_decode($this->ReadAttributeString(self::BUFFER_IDENTIFIER_MAP), true) ?? [];
    }

    /**
     * Stores the identifier map.
     *
     * @param array<string, int> $map
     */
    private function SetIdentifierMap(array $map): void
    {
        $this->WriteAttributeString(self::BUFFER_IDENTIFIER_MAP, json_encode($map));
    }

    /**
     * Fetches cached profiles.
     *
     * @return array<string, array>
     */
    private function GetProfileCache(): array
    {
        return json_decode($this->ReadAttributeString(self::BUFFER_PROFILE_CACHE), true) ?? [];
    }

    /**
     * Stores profile cache.
     *
     * @param array<string, array> $cache
     */
    private function SetProfileCache(array $cache): void
    {
        $this->WriteAttributeString(self::BUFFER_PROFILE_CACHE, json_encode($cache));
    }

    /**
     * Fetches pending actions.
     *
     * @return array<string, int>
     */
    private function GetPendingActions(): array
    {
        return json_decode($this->ReadAttributeString(self::BUFFER_PENDING_ACTIONS), true) ?? [];
    }

    /**
     * Stores pending actions.
     *
     * @param array<string, int> $pending
     */
    private function SetPendingActions(array $pending): void
    {
        $this->WriteAttributeString(self::BUFFER_PENDING_ACTIONS, json_encode($pending));
    }

    /**
     * Retrieves processing buffer.
     *
     * @return array<int, array{token:string, reason:string}>
     */
    private function GetProcessing(): array
    {
        return json_decode($this->ReadAttributeString(self::BUFFER_PROCESSING), true) ?? [];
    }

    /**
     * Stores processing buffer.
     *
     * @param array<int, array{token:string, reason:string}> $processing
     */
    private function SetProcessing(array $processing): void
    {
        $this->WriteAttributeString(self::BUFFER_PROCESSING, json_encode($processing));
    }

    /**
     * Retrieves known profiles.
     *
     * @return array<int, string>
     */
    private function GetKnownProfiles(): array
    {
        return json_decode($this->ReadAttributeString(self::BUFFER_KNOWN_PROFILES), true) ?? [];
    }

    /**
     * Stores known profiles.
     *
     * @param array<int, string> $profiles
     */
    private function SetKnownProfiles(array $profiles): void
    {
        $this->WriteAttributeString(self::BUFFER_KNOWN_PROFILES, json_encode($profiles));
    }

    /**
     * Serialises a value for MQTT transfer.
     *
     * @param mixed $value
     *
     * @return mixed
     */
    private function SerializeValueForTransmission($value)
    {
        if (is_string($value) || is_bool($value) || is_int($value) || is_float($value) || $value === null) {
            return $value;
        }

        return json_encode($value);
    }

    /**
     * Deserialises an incoming value according to the variable type.
     *
     * @param mixed $value
     * @param int   $type
     *
     * @return mixed
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
                return (string) $value;
        }
    }

    /**
     * Returns object name including custom name.
     *
     * @param int $objectID
     *
     * @return string
     */
    private function GetObjectName(int $objectID): string
    {
        $object = IPS_GetObject($objectID);
        return $object['ObjectName'];
    }

    /**
     * Builds a prefixed profile name.
     *
     * @param string $profileName
     *
     * @return string
     */
    private function BuildPrefixedProfileName(string $profileName): string
    {
        $prefix = $this->ReadPropertyString('ProfilePrefix');
        return $prefix . $profileName;
    }

    /**
     * Extended debug helper with optional structured data.
     *
     * @param string     $context
     * @param string     $message
     * @param mixed|null $data
     */
    private function SendDebugExtended(string $context, string $message, $data = null): void
    {
        if (!$this->ReadPropertyBoolean('EnableDebug')) {
            return;
        }

        if ($data !== null) {
            $message .= ' | ' . json_encode($data);
        }
        $this->SendDebug($context, $message, 0);
    }
}

