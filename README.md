# MQTT Variable Sync für IP-Symcon

Dieses Repository enthält das IP-Symcon-Modul **MQTTVariableSync**, das Variablen
und Variablenprofile zwischen zwei IP-Symcon-Instanzen bidirektional
synchronisiert. Die Kommunikation erfolgt über einen frei konfigurierbaren
MQTT-Broker und kann sowohl mit einer MQTTClient- als auch einer
MQTTServer-Instanz gekoppelt werden.

## Funktionsumfang

- Spiegelung kompletter Objektbäume (Kategorien/Geräte) inklusive aller
  enthaltenen Variablen.
- Automatische Übertragung von Variablenprofilen mit konfigurierbarem Präfix
  (`ProfilePrefix`).
- Bidirektionale Wert-Synchronisation mit separaten MQTT-Topics für Sende- und
  Empfangsrichtung zur Loop-Vermeidung.
- Weiterleitung von `RequestAction`-Aufrufen an die Originalinstanz inklusive
  Rückmeldung über Erfolg bzw. Fehlermeldung.
- Umfangreiches Debug- und Aktions-Logging, jeweils einzeln konfigurierbar.
- Timer-gestützte Initial-Synchronisation sowie manuell anstoßbare Vollsynchro-
  nisation über das Konfigurationsformular.

## Systemvoraussetzungen

- IP-Symcon 6.0 oder neuer (PHP 7.4+).
- Eine verfügbare MQTT-Infrastruktur (lokaler oder externer Broker) oder eine
  IP-Symcon-MQTTServer-Instanz.
- Schreib-/Leserechte auf beiden IP-Symcon-Instanzen für alle zu
  synchronisierenden Objekte.

## Installation

1. Repository in das IP-Symcon-Modulverzeichnis klonen oder über den Module
   Store einbinden.
2. Instanz **MQTT Variable Sync** anlegen.
3. Entsprechend `Client` oder `Server` als Betriebsmodus wählen.
4. MQTT-Verbindungsdaten (Broker, Port, Benutzer, Passwort) im Formular
   hinterlegen.
5. Mirror-Wurzel (`MirrorRoot`) sowie die zu synchronisierenden Kategorien oder
   Geräte (`SyncTargets`) auswählen.
6. Optional: Debug- und Aktions-Logging aktivieren.
7. Instanz übernehmen; nach 5 Sekunden startet die Initial-Synchronisation
   automatisch.

## Konfigurationseigenschaften

| Eigenschaft        | Beschreibung                                                                 |
|--------------------|------------------------------------------------------------------------------|
| `Mode`             | Betrieb als `Client` (Verbindung zu externem Broker) oder `Server`.          |
| `BrokerHost`       | Hostname/IP des Brokers (nur im Client-Modus relevant).                      |
| `BrokerPort`       | Port für den MQTT-Verkehr.                                                   |
| `Username`/`Password` | Anmeldedaten für den Broker.                                              |
| `TopicPrefix`      | Präfix für logische Gruppierung der Topics.                                  |
| `SendTopic`        | Topic, auf dem lokale Änderungen gesendet werden.                            |
| `ReceiveTopic`     | Topic, auf dem entfernte Änderungen empfangen werden.                        |
| `ProfilePrefix`    | Präfix für automatisch angelegte Variablenprofile auf der Gegenseite.        |
| `EnableDebug`      | Aktiviert JSON-basiertes Debug-Logging.                                      |
| `EnableActionLog`  | Schreibt Erfolg/Fehlschlag von `RequestAction`-Aufrufen in das Symcon-Log.   |
| `MirrorRoot`       | Kategorie, unter der Spiegel-Objekte erstellt werden.                        |
| `SyncTargets`      | Liste zu synchronisierender Kategorien/Geräte (inkl. Unterelementen).        |

## Ablauf der Synchronisation

1. **Initialer Scan:** Alle in `SyncTargets` enthaltenen Strukturen werden
   rekursiv durchsucht. Variablen werden mitsamt Profilinformationen an die
   Gegenseite publiziert.
2. **Profilabgleich:** Vor der ersten Werteübertragung prüft das Modul, ob das
   Profil vorhanden ist. Falls nicht, wird eine Kopie mit dem Präfix aus
   `ProfilePrefix` angelegt.
3. **Laufender Betrieb:** Änderungen an lokalen Variablen lösen über `VM_UPDATE`
   sofort eine Übertragung aus. Auf der Gegenseite werden Werte gesetzt,
   sofern die Variable nicht bereits durch den selben Transfer in Bearbeitung
   ist (`processing`-Flag).
4. **Aktionen:** Wird auf der Spiegel-Seite ein Wert geschrieben, ruft das Modul
   `RequestAction` der Originalvariable auf. Das boolean Ergebnis wird als
   `actionResult`-Nachricht zurückgesendet und optional geloggt.

## MQTT-Nachrichtenstruktur

Alle Nutzdaten werden JSON-kodiert übertragen. Wichtige Nachrichtentypen:

- `profile`: Enthält eine vollständige Profildefinition (`Name`, `Type`,
  Wertebereich, Assoziationen).
- `variableUpdate`: Kombiniert Definition (Name, Typ, Profil, Ident, Pfad) und
  aktuellen Wert.
- `setValue`: Anforderung, auf der Gegenseite `RequestAction` auszuführen.
- `actionResult`: Rückmeldung zum Erfolg/Fehlschlag eines Setz-Versuchs.

Sende- und Empfangs-Topic werden getrennt verwaltet, sodass jedes System klar
zwischen ausgehenden und eingehenden Nachrichten unterscheidet.

## Logging und Debugging

- **Debug-Log (`EnableDebug`):** Ausführliche JSON-Ausgaben je Nachricht und
  Verarbeitungsschritt. Ideal für die Fehlersuche bei Verbindungsproblemen.
- **Aktions-Log (`EnableActionLog`):** Protokolliert das Ergebnis von
  `RequestAction`-Aufrufen mit Identifier und Fehlermeldung.

Beide Optionen lassen sich unabhängig voneinander aktivieren.

## Fehlerbehebung

| Problem                              | Ursache / Lösungsvorschlag                                      |
|--------------------------------------|-----------------------------------------------------------------|
| Keine Werteübertragung               | Topics prüfen, Receive-Filter kontrollieren, Broker erreichbar? |
| Profile fehlen auf Gegenseite        | `ProfilePrefix` korrekt gesetzt? Profil-Cache leeren.           |
| Schleifen trotz getrennter Topics    | Prüfen, ob Send-/Receive-Topic vertauscht wurden.               |
| `RequestAction` schlägt fehl         | Log-Ausgabe lesen, Zielvariable unterstützt die Aktion?         |

## Entwicklung & Tests

- Syntax-Prüfung mittels `php -l MQTTVariableSync/module.php`.
- Weitere Tests können über IP-Symcon-Testumgebungen oder automatisierte
  Integrationstests erfolgen.

## Lizenz

Die Nutzung richtet sich nach den Lizenzbestimmungen der IP-Symcon GmbH und
den individuellen Vorgaben des jeweiligen Projekts. Eine gesonderte Lizenzdatei
liegt diesem Repository nicht bei.
