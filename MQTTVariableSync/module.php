<?php

declare(strict_types=1);

/**
 * IP-Symcon Modul zur bidirektionalen Synchronisation von Variablen und Profilen über MQTT.
 *
 * Das Modul verwendet einen eindeutigen Instanz-Bezeichner in den MQTT-Topics,
 * um Feedback-Loops (Schleifen) effektiv zu verhindern.
 *
 * Das Modul benötigt den MQTT Client als Parent.
 *
 * Wichtig: Alle Variablen-IDs (SourceID) in den Topics sind die IDs der
 * Variablen auf der Seite, wo die Variable *ursprünglich* existiert.
 *
 * @author Gemini
 */
class MQTTVariableSync extends IPSModule

{
    // Temporäre Flagge zur Vermeidung von Rückkopplungsschleifen beim Empfangen
    private $isReceiving = false;

    /**
     * Erforderliche Funktion des MQTT Clients, wird beim Empfang von Daten auf den abonnierten Topics aufgerufen.
     * @param string $JSONString JSON-Daten der empfangenen Nachricht.
     */
    public function ReceiveData($JSONString)
    {
        $this->SendDebug('ReceiveData', 'Daten empfangen: ' . $JSONString, 0);

        // Daten dekodieren
        $data = json_decode($JSONString);

        // Sicherstellen, dass die Datenstruktur korrekt ist
        if (!isset($data->DataID) || $data->DataID != '{018EF6B5-AB94-40C6-AA53-469411E7EE0A}' || !isset($data->Topic)) {
            return;
        }

        $topic = (string)$data->Topic;
        $payload = (string)$data->Payload;

        // Basis-Topic Präfix
        $prefix = $this->ReadPropertyString('Topic');
        if (empty($prefix)) {
            $this->SendDebug('ReceiveData', 'Topic Präfix ist leer. Ignoriere Nachricht.', 0);
            return;
        }

        // --- 1. Loop-Prevention und Topic-Parsing ---

        $parts = explode('/', $topic);

        // Erwartete Struktur: {prefix}/sync|action|feedback/{senderID}/{sourceVarID}
        if (count($parts) < 5 || $parts[0] !== $prefix) {
            $this->SendDebug('ReceiveData', 'Ungültiges Topic-Format: ' . $topic, 0);
            return;
        }

        $type = $parts[1]; // sync, action, feedback
        $senderID = (int)$parts[2];
        $sourceVarID = (int)$parts[3];
        $targetIdent = $parts[4]; // Wird nur für Feedback verwendet

        // Hole die ID des verbundenen MQTT-Client-Instanzobjekts
        $clientID = IPS_GetObjectIDByIdent('Client', $this->InstanceID);

        // ACHTUNG: Loop-Prevention (Ignoriere eigene Echos vom Broker)
        if ($senderID === $this->InstanceID) {
            $this->SendDebug('ReceiveData', "Nachricht von eigener Instanz ($senderID) ignoriert (Echo-Prevention).", 0);
            return;
        }

        // Liste der synchronisierten Variablen abrufen
        $syncedVariables = json_decode($this->GetBuffer('SyncedVariables'), true) ?: [];
        $varData = null;

        // Finde die entsprechende Variable im lokalen Puffer (entweder SourceID oder TargetID)
        foreach ($syncedVariables as $item) {
            // Wir suchen nach der SourceID, da diese im Topic übermittelt wird
            if ($item['SourceID'] == $sourceVarID) {
                $varData = $item;
                break;
            }
        }

        if ($varData === null) {
            $this->SendDebug('ReceiveData', "Keine synchronisierte Variable für Quell-ID $sourceVarID gefunden. Ignoriere.", 0);
            return;
        }

        // Konvertiere den Payload zum korrekten Typ (IP-Symcon speichert Typen als 1=Bool, 2=Int, 3=Float, 4=String)
        $value = $this->ConvertPayloadToValue($payload, $varData['VariableType']);
        $this->SendDebug('ReceiveData', "Empfangener Wert für $sourceVarID ($type): " . print_r($value, true), 0);

        // --- 2. Verarbeitung nach Typ ---

        if ($type === 'sync') {
            // Dies ist ein Wert-Update von der Gegenseite.
            if ($varData['TargetID'] === 0) {
                 $this->SendDebug('ReceiveData', "Sync-Nachricht für Variable $sourceVarID, aber keine TargetID lokal definiert. Ignoriere.", 0);
                 return;
            }

            // Schleifenschutz: Flag setzen, damit das Setzen des Werts kein neues Outgoing-Event auslöst
            $this->isReceiving = true;
            if (is_string($value) && $varData['VariableType'] !== 3) {
                 // IPS_SetVariableState erwartet keinen String für numerische oder boolesche Typen
                 $value = json_decode($value);
            }
            
            $result = @IPS_SetVariableState($varData['TargetID'], $value);
            $this->isReceiving = false;

            if ($result) {
                $this->SendDebug('ReceiveData', "Wert für TargetID {$varData['TargetID']} erfolgreich auf $payload aktualisiert.", 0);
            } else {
                IPS_LogMessage('IPSSyncMQTT', "Fehler beim Aktualisieren des Werts für TargetID {$varData['TargetID']} ($sourceVarID).");
            }

        } elseif ($type === 'action') {
            // Dies ist eine RequestAction-Anforderung von der Gegenseite.
            $this->SendDebug('ReceiveData', "RequestAction-Anforderung für lokale Quell-Variable $sourceVarID erhalten.", 0);

            // 9. Auf der Originalseite RequestAction verwenden
            $success = @RequestAction($sourceVarID, $value);

            // 10. Boolean-Ergebnis zurückmelden
            $feedbackTopic = "$prefix/feedback/{$this->InstanceID}/$sourceVarID/".$varData['TargetIdent'];

            // Sende das boolean-Ergebnis zurück an die Gegenseite
            $this->ForwardData(json_encode([
                'DataID' => '{018EF6B5-AB94-40C6-AA53-469411E7EE0A}',
                'Topic'  => $feedbackTopic,
                'Payload' => json_encode($success)
            ]));
            $this->SendDebug('ReceiveData', "RequestAction-Ergebnis ($success) an $feedbackTopic gesendet.", 0);

        } elseif ($type === 'feedback') {
            // Dies ist das Ergebnis einer RequestAction-Ausführung auf der Gegenseite.

            $feedback = json_decode($payload); // Sollte ein Boolean sein

            // 11. Rückmeldung protokollieren
            if ($this->ReadPropertyBoolean('LogFeedback')) {
                $msg = $feedback ?
                    "RequestAction für Variable '$sourceVarID' (lokal TargetID {$varData['TargetID']}) auf Gegenseite erfolgreich ausgeführt." :
                    "RequestAction für Variable '$sourceVarID' (lokal TargetID {$varData['TargetID']}) auf Gegenseite fehlgeschlagen!";
                
                $this->SendDebug('ReceiveData', $msg, 0);
                IPS_LogMessage('IPSSyncMQTT', $msg);
            }
        }
    }

    /**
     * Konvertiert den JSON-Payload in den korrekten PHP-Werttyp (basierend auf dem IP-Symcon Typ).
     * @param string $payload Der empfangene String-Payload.
     * @param int $type Der IP-Symcon Variablentyp (1=Bool, 2=Int, 3=Float, 4=String).
     * @return mixed Der konvertierte Wert.
     */
    protected function ConvertPayloadToValue(string $payload, int $type)
    {
        switch ($type) {
            case 1: // Boolean
                return filter_var($payload, FILTER_VALIDATE_BOOLEAN);
            case 2: // Integer
                return (int)json_decode($payload);
            case 3: // Float
                return (float)json_decode($payload);
            case 4: // String
            default:
                // String-Typen werden oft als JSON-String gesendet und müssen dekodiert werden
                $decoded = json_decode($payload);
                return is_string($decoded) ? $decoded : $payload; 
        }
    }


    // ----------------------------------------------------------------------------------
    // Standard Modul-Funktionen
    // ----------------------------------------------------------------------------------

    public function Create()
    {
        // Never delete this part!
        parent::Create();

        // Register Properties
        $this->RegisterPropertyString('Topic', 'ips/sync');
        $this->RegisterPropertyString('SyncRoots', '[]');
        $this->RegisterPropertyString('SyncedVariablesList', '[]');
        $this->RegisterPropertyBoolean('LogDebug', false);
        $this->RegisterPropertyBoolean('LogFeedback', true);
        
        // Register Buffer
        $this->SetBuffer('SyncedVariables', '[]');

        // Register Variables
        $this->RegisterVariableInteger('LastSyncTime', 'Letzte Sync Zeit', '~UnixTimestamp', 1);

        // Erforderlich, da wir den MQTT Client als Parent haben
        $this->ConnectParent('{F7A0DD2E-7684-95C0-64C2-D2A9DC47577B}');
    }

    public function Destroy()
    {
        // Never delete this part!
        parent::Destroy();
    }

    public function ApplyChanges()
    {
        // Never delete this part!
        parent::ApplyChanges();

        // Debug-Logging ein-/ausschalten
        $this->SetDebugMode($this->ReadPropertyBoolean('LogDebug'));
        
        // --- 1. Subskriptionen einrichten ---
        $this->SetupSubscriptions();

        // --- 2. Synchronisationsprozess starten ---
        $this->StartSynchronization();
    }
    
    // ----------------------------------------------------------------------------------
    // Interne Synchronisations-Methoden
    // ----------------------------------------------------------------------------------

    /**
     * Richtet die notwendigen MQTT-Subskriptionen ein.
     */
    protected function SetupSubscriptions()
    {
        $prefix = $this->ReadPropertyString('Topic');
        if (empty($prefix)) {
            $this->SetStatus(104); // Ungültige Konfiguration
            return;
        }
        $this->SetStatus(102); // Instanz ist aktiv
        
        // Subskription für ALLE Sync-Nachrichten von ANDEREN Instanzen
        $topics[] = $prefix . '/sync/+/+';
        
        // Subskription für ALLE Action-Anforderungen (RequestAction auf lokaler Quell-Var)
        $topics[] = $prefix . '/action/+';
        
        // Subskription für ALLE Feedback-Nachrichten (Ergebnis von RequestAction)
        $topics[] = $prefix . '/feedback/+/+/+'; 
        
        // Payload an Parent (MQTT Client) senden
        $this->SetReceiveDataFilter(json_encode($topics));
        $this->SendDebug('SetupSubscriptions', 'Subskribiert auf Topics: ' . print_r($topics, true), 0);
    }
    
    /**
     * Startet den rekursiven Scan und die Synchronisation.
     */
    protected function StartSynchronization()
    {
        $rootIDs = json_decode($this->ReadPropertyString('SyncRoots'));
        if (empty($rootIDs)) {
            $this->SendDebug('StartSynchronization', 'Keine Synchronisations-Quellen ausgewählt.', 0);
            return;
        }

        // Aktuellen Synchronisations-Puffer löschen und neu aufbauen
        $this->SetBuffer('SyncedVariables', '[]');
        $newSyncedVariables = [];
        
        // Alle bestehenden Event-Registrierungen löschen
        $this->UnregisterAllEvents();

        foreach ($rootIDs as $rootID) {
            if ($rootID === 0) continue;
            $this->SyncStructureRecursively($rootID, 0, $newSyncedVariables);
        }

        // Puffer speichern und Statusvariable aktualisieren
        $this->SetBuffer('SyncedVariables', json_encode($newSyncedVariables));
        $this->SetVariable('LastSyncTime', time());
        
        // SyncedVariablesList für die Form speichern
        $this->UpdateFormField('SyncedVariablesList', 'values', json_encode($newSyncedVariables));

        $this->SendDebug('StartSynchronization', 'Synchronisation abgeschlossen. ' . count($newSyncedVariables) . ' Variablen registriert.', 0);
    }

    /**
     * Löscht alle vom Modul registrierten Variablen-Events.
     */
    protected function UnregisterAllEvents()
    {
        $childrenIDs = IPS_GetChildrenIDs($this->InstanceID);
        foreach ($childrenIDs as $childID) {
            $objectIdent = IPS_GetObject($childID)['ObjectIdent'];
            // Alle Events, die mit 'VSYNC' beginnen, löschen
            if (strpos($objectIdent, 'VSYNC') === 0) {
                @IPS_DeleteEvent($childID);
                $this->SendDebug('UnregisterAllEvents', 'Event ' . $childID . ' (' . $objectIdent . ') gelöscht.', 0);
            }
        }
    }

    /**
     * Rekursive Funktion zum Durchlaufen der Objektstruktur und zur Synchronisation.
     * @param int $sourceID Die ID des zu synchronisierenden Objekts (Kategorie/Variable).
     * @param int $parentTargetID Die ID des übergeordneten Objekts auf der Zielseite (Target-Instanz).
     * @param array $newSyncedVariables Array zur Speicherung der synchronisierten Variablen-Daten.
     */
    protected function SyncStructureRecursively(int $sourceID, int $parentTargetID, array &$newSyncedVariables)
    {
        $object = IPS_GetObject($sourceID);
        if ($object === false) return;

        // 6. Links ignorieren
        if ($object['ObjectIsLink']) {
            $this->SendDebug('SyncStructureRecursively', 'Link (' . $sourceID . ') ignoriert.', 0);
            return;
        }

        if ($object['ObjectType'] === 6) { // Instanz (z.B. Gerät)
            $this->SendDebug('SyncStructureRecursively', 'Gerät (' . $object['ObjectName'] . ') synchronisiert.', 0);
            
            // 7. Struktur der Kategorien/Geräte auf der Gegenseite analog aufbauen
            $newTargetID = $this->SyncCategoryOrInstance($sourceID, $parentTargetID, $object);
            
            // Rekursiv alle Kinder des Geräts/der Kategorie synchronisieren
            foreach (IPS_GetChildrenIDs($sourceID) as $childID) {
                $this->SyncStructureRecursively($childID, $newTargetID, $newSyncedVariables);
            }
        } elseif ($object['ObjectType'] === 3) { // Variable
            // 5. Bereits im Synchronisationsprozess befindliche Variablen überspringen
            // Das wird durch die Neuanlage des Puffers automatisch behandelt, aber wir können noch einmal prüfen, ob sie in einer früheren Root-ID enthalten war.
            foreach ($newSyncedVariables as $item) {
                if ($item['SourceID'] === $sourceID) {
                    $this->SendDebug('SyncStructureRecursively', 'Variable ' . $object['ObjectName'] . ' (ID ' . $sourceID . ') bereits synchronisiert. Übersprungen.', 0);
                    return;
                }
            }
            
            $this->SyncVariable($sourceID, $parentTargetID, $newSyncedVariables);
        } elseif ($object['ObjectType'] === 1) { // Kategorie
            $this->SendDebug('SyncStructureRecursively', 'Kategorie (' . $object['ObjectName'] . ') synchronisiert.', 0);
            
            // 7. Struktur der Kategorien/Geräte auf der Gegenseite analog aufbauen
            $newTargetID = $this->SyncCategoryOrInstance($sourceID, $parentTargetID, $object);

            // Rekursiv alle Kinder der Kategorie synchronisieren
            foreach (IPS_GetChildrenIDs($sourceID) as $childID) {
                $this->SyncStructureRecursively($childID, $newTargetID, $newSyncedVariables);
            }
        }
        // Andere Objekttypen (z.B. Skripte, Medien, Ereignisse) werden ignoriert
    }
    
    /**
     * Synchronisiert eine Kategorie oder Instanz (Gerät).
     * @param int $sourceID Die ID des Quellobjekts.
     * @param int $parentTargetID Die ID des Ziel-Elternteils (lokale Modul-ID oder erstellte Kategorie).
     * @param array $sourceObject Das IPS_GetObject Array des Quellobjekts.
     * @return int Die ID des lokal erstellten Zielobjekts.
     */
    protected function SyncCategoryOrInstance(int $sourceID, int $parentTargetID, array $sourceObject)
    {
        $ident = 'SYNC_' . $sourceID;
        
        // Wenn $parentTargetID 0 ist, ist der Parent die Modul-Instanz selbst
        $parentID = $parentTargetID === 0 ? $this->InstanceID : $parentTargetID;

        // Versuche, das Zielobjekt zu finden
        $targetID = @IPS_GetObjectIDByIdent($ident, $parentID);

        // Erstelle das Zielobjekt, falls es nicht existiert
        if ($targetID === false) {
            if ($sourceObject['ObjectType'] === 1) { // Kategorie
                $targetID = IPS_CreateCategory();
            } else { // Instanz (wird als Kategorie auf der Gegenseite abgebildet)
                $targetID = IPS_CreateCategory();
            }
            IPS_SetName($targetID, $sourceObject['ObjectName']);
            IPS_SetParent($targetID, $parentID);
            IPS_SetIdent($targetID, $ident);
            $this->SendDebug('SyncCategoryOrInstance', 'Neues Zielobjekt erstellt: ' . $sourceObject['ObjectName'] . ' (ID: ' . $targetID . ')', 0);
        } else {
            // Namen synchronisieren, falls geändert
            IPS_SetName($targetID, $sourceObject['ObjectName']);
        }
        
        return $targetID;
    }

    /**
     * Synchronisiert eine einzelne Variable.
     * @param int $sourceID Die ID der Quellvariable.
     * @param int $parentTargetID Die ID des Ziel-Elternteils (lokal erstellt).
     * @param array $newSyncedVariables Array zur Speicherung der synchronisierten Variablen-Daten.
     */
    protected function SyncVariable(int $sourceID, int $parentTargetID, array &$newSyncedVariables)
    {
        $sourceVar = IPS_GetVariable($sourceID);
        $sourceObject = IPS_GetObject($sourceID);
        
        // 1. Variablenprofil synchronisieren/erstellen
        $profileName = $this->SyncProfile($sourceVar);

        // 7. Ziel-Identifikator und übergeordnetes Objekt
        $ident = $sourceObject['ObjectIdent'] !== '' ? $sourceObject['ObjectIdent'] : $sourceObject['ObjectName'];
        $targetIdent = 'SYNC_VAR_' . $sourceID;
        $parentID = $parentTargetID === 0 ? $this->InstanceID : $parentTargetID;

        // Zielvariable suchen
        $targetID = @IPS_GetObjectIDByIdent($targetIdent, $parentID);

        // Zielvariable erstellen/aktualisieren
        if ($targetID === false) {
            $targetID = IPS_CreateVariable($sourceVar['VariableType']);
            IPS_SetName($targetID, $sourceObject['ObjectName']);
            IPS_SetParent($targetID, $parentID);
            IPS_SetIdent($targetID, $targetIdent);
            IPS_SetVariableCustomProfile($targetID, $profileName);
            $this->SendDebug('SyncVariable', 'Neue Zielvariable erstellt: ' . $sourceObject['ObjectName'] . ' (ID: ' . $targetID . ')', 0);
        } else {
            // Variable synchronisiert bereits, nur aktualisieren
            IPS_SetVariableCustomProfile($targetID, $profileName);
            IPS_SetVariableType($targetID, $sourceVar['VariableType']);
            IPS_SetName($targetID, $sourceObject['ObjectName']);
            $this->SendDebug('SyncVariable', 'Zielvariable aktualisiert: ' . $sourceObject['ObjectName'] . ' (ID: ' . $targetID . ')', 0);
        }

        // Puffer-Eintrag erstellen
        $newSyncedVariables[] = [
            'SourceID'      => $sourceID,
            'Name'          => $sourceObject['ObjectName'],
            'TargetID'      => $targetID,
            'ProfileName'   => $profileName,
            'VariableType'  => $sourceVar['VariableType'],
            'TargetIdent'   => $targetIdent, // Zur besseren Identifikation beim Feedback
        ];

        // 2. Event-Listener für Quell- und Zielvariable registrieren

        // a) Event für Quellvariable (SourceID) -> Sendet Werte/Commands an Remote
        // Name: Senden (Sync)
        $this->RegisterVariableEvent($sourceID, $sourceVar, 'sync', $targetID);
        
        // b) Event für Zielvariable (TargetID) -> Sendet Commands ZURÜCK an Source
        // Name: Befehl (Action)
        if ($sourceVar['VariableCustomAction'] != 0 || $sourceVar['VariableAction'] != 0) {
             $this->RegisterVariableEvent($targetID, $sourceVar, 'action', $sourceID);
        }
    }

    /**
     * Registriert ein Ereignis auf einer Variable.
     * @param int $monitorID Die ID der Variable, die überwacht wird.
     * @param array $sourceVar Das IPS_GetVariable Array der Quellvariable.
     * @param string $type Typ des Events ('sync' oder 'action').
     * @param int $oppositeID Die ID der Gegenseite (SourceID bei Target-Event, TargetID bei Source-Event).
     */
    protected function RegisterVariableEvent(int $monitorID, array $sourceVar, string $type, int $oppositeID)
    {
        // Event-Ident: Eindeutig und an die Modul-Instanz gebunden
        $eventIdent = 'VSYNC_' . $type . '_' . $monitorID;

        $eventID = @IPS_GetObjectIDByIdent($eventIdent, $this->InstanceID);
        
        // Wenn das Event nicht existiert, neu erstellen
        if ($eventID === false) {
            $eventID = IPS_CreateEvent(0); // 0 = Standard Event
            IPS_SetIdent($eventID, $eventIdent);
            IPS_SetParent($eventID, $this->InstanceID);
            IPS_SetName($eventID, 'Sync ' . $type . ' von ID ' . $monitorID);
        }
        
        // Skript-Inhalt setzen (Code, der beim Event ausgeführt wird)
        $scriptContent = sprintf(
            '$this->SendValueOrAction(%d, %d, $VariableValue, "%s");',
            $monitorID,
            $oppositeID,
            $type
        );
        
        IPS_SetEventScript($eventID, $scriptContent);
        IPS_SetEventTrigger($eventID, 1, $monitorID); // Trigger: Bei Änderung des Variablenwerts
        IPS_SetEventActive($eventID, true);
        
        $this->SendDebug('RegisterVariableEvent', "Event vom Typ '$type' für Variable $monitorID registriert.", 0);
    }
    
    /**
     * Sendet den Wert oder eine RequestAction-Anforderung über MQTT.
     * Wird vom Event-Skript aufgerufen.
     * @param int $sourceID Die ID der Variable, deren Wert sich geändert hat.
     * @param int $oppositeID Die ID der Gegenseite (für Logging, etc.).
     * @param mixed $value Der neue Wert der Variable.
     * @param string $type 'sync' (Wert-Update) oder 'action' (RequestAction-Anforderung).
     */
    public function SendValueOrAction(int $sourceID, int $oppositeID, $value, string $type)
    {
        // Schleifenschutz: Wenn die Änderung vom ReceiveData-Handler stammt, nichts senden
        if ($this->isReceiving) {
            return;
        }

        $prefix = $this->ReadPropertyString('Topic');
        if (empty($prefix)) return;
        
        $payload = json_encode($value);

        if ($type === 'sync') {
            // 2. und 8. Änderungen an Variablen synchronisieren
            // Topic-Struktur: {prefix}/sync/{senderID}/{sourceVarID}/{targetIdent}
            // Der TargetIdent wird nicht zwingend im Sync-Topic benötigt, aber zur Konsistenz lassen wir ihn drin
            $topic = "$prefix/sync/{$this->InstanceID}/$sourceID/S_VAR_$oppositeID";
            $this->SendDebug('SendValueOrAction', "Sende Wert-Sync für $sourceID ($value) an $topic", 0);
        } elseif ($type === 'action') {
            // 9. und 8. Bidirektionale Steuerung: Befehl zur RequestAction auf der Original-Variable
            // Topic-Struktur: {prefix}/action/{senderID}/{sourceVarID}/{targetIdent}
            // Hier ist $sourceID die *Target*-Variable, die das Action-Event auslöste. $oppositeID ist die *Original*-Variable.
            $topic = "$prefix/action/{$this->InstanceID}/$oppositeID/S_VAR_$sourceID";
            $this->SendDebug('SendValueOrAction', "Sende RequestAction-Befehl für $oppositeID ($value) an $topic", 0);
        } else {
            return;
        }

        // Payload an Parent (MQTT Client) senden
        $this->ForwardData(json_encode([
            'DataID' => '{018EF6B5-AB94-40C6-AA53-469411E7EE0A}',
            'Topic'  => $topic,
            'Payload' => $payload
        ]));
    }
    
    /**
     * Synchronisiert das Variablenprofil der Quellvariable und erstellt es bei Bedarf auf der Gegenseite.
     * @param array $sourceVar Das IPS_GetVariable Array der Quellvariable.
     * @return string Der verwendete (ggf. neu erstellte) Profilname.
     */
    protected function SyncProfile(array $sourceVar): string
    {
        $currentProfileName = $sourceVar['VariableCustomProfile'] !== '' ? $sourceVar['VariableCustomProfile'] : $sourceVar['VariableProfile'];

        // 1. Profil existiert bereits oder ist leer, direkt verwenden
        if ($currentProfileName === '' || @IPS_VariableProfileExists($currentProfileName)) {
            return $currentProfileName;
        }

        // 1. Eindeutiges Präfix verwenden
        $newProfileName = 'MQTT_' . $currentProfileName;

        // 1. Verhindern des doppelten Anlegens
        if (@IPS_VariableProfileExists($newProfileName)) {
            $this->SendDebug('SyncProfile', "Zielprofil $newProfileName existiert bereits.", 0);
            return $newProfileName;
        }
        
        $this->SendDebug('SyncProfile', "Erstelle neues Variablenprofil: $newProfileName", 0);

        // Profil-Daten abrufen
        $profile = IPS_GetVariableProfile($currentProfileName);

        // Neues Profil erstellen
        @IPS_CreateVariableProfile($newProfileName, $sourceVar['VariableType']);

        // Allgemeine Eigenschaften
        @IPS_SetVariableProfileText($newProfileName, $profile['Prefix'], $profile['Suffix']);
        @IPS_SetVariableProfileDigits($newProfileName, $profile['Digits']);
        @IPS_SetVariableProfileIcon($newProfileName, $profile['Icon']);

        if ($sourceVar['VariableType'] !== 1) { // Nicht für Boolean
            @IPS_SetVariableProfileValues($newProfileName, $profile['MinValue'], $profile['MaxValue'], $profile['StepSize']);
        }

        // Assoziationen (Status-Texte) kopieren
        if ($profile['Associations']) {
            foreach ($profile['Associations'] as $assoc) {
                @IPS_SetVariableProfileAssociation($newProfileName, $assoc[0], $assoc[1], $assoc[2], $assoc[3]);
            }
        }
        
        return $newProfileName;
    }

    /**
     * Hilfsfunktion zum Setzen von Instanz-Variablen.
     */
    protected function SetVariable(string $ident, $value)
    {
        $id = $this->GetIDForIdent($ident);
        if ($id !== 0) {
            $this->SetValue($ident, $value);
        }
    }
    
    /**
     * Hilfsfunktion zum Setzen des Debug-Modus.
     * @param bool $active Ob Debug-Logging aktiv sein soll.
     */
    protected function SetDebugMode(bool $active)
    {
        if (IPS_GetDebug($this->InstanceID) !== $active) {
            IPS_SetDebug($this->InstanceID, $active);
        }
    }
}
