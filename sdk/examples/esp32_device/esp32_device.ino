// A2A Mesh Device SDK — ESP32 (Arduino/Pio) MQTT-kliens példa
// ============================================================
// A MeshDevice (sdk/mqtt_device.py) C++-megfelelője az end-device architektúra szerint:
//  - manifest-publish (retained) a a2a/devices/{device_id}/manifest topicra
//  - LWT: a2a/devices/{device_id}/status = "offline" (broker közzéteszi kilépéskor)
//  - szenzor-adatok: a2a/devices/{device_id}/telemetry/{sensor}
//  - DM a node-okhoz: a2a/chat/dm/{node} (A2AMessage JSON envelope)
//  - parancs-feliratkozás: a2a/devices/{device_id}/cmd/#
//
// Könyvtár: PubSubClient (Arduino Library Managerből) + WiFi
// Broker: Nova 192.168.1.8:8683 (LAN, allow_anonymous MVP — mTLS később)

#include <WiFi.h>
#include <PubSubClient.h>
#include <ArduinoJson.h>

const char* WIFI_SSID = "YOUR_SSID";
const char* WIFI_PASS = "YOUR_PASS";
const char* MQTT_HOST = "192.168.1.8";
const int   MQTT_PORT = 8683;

const char* DEVICE_ID  = "esp32-msensor";
const char* DEVICE_NAME = "MSensor v2.6.0";

WiFiClient net;
PubSubClient mqtt(net);

// ── Topic-helperk ──────────────────────────────────────────
String topic(const char* kind) {           // kind: manifest/status/telemetry/cmd
  return String("a2a/devices/") + DEVICE_ID + "/" + kind;
}
String dmTopic(const char* node) { return String("a2a/chat/dm/") + node; }

// ── Manifest (retained) — a device azonosító kártyája ──────
void publishManifest() {
  StaticJsonDocument<512> doc;
  doc["device_id"]  = DEVICE_ID;
  doc["name"]       = DEVICE_NAME;
  doc["type"]       = "sensor";
  doc["node"]       = "";                    // a fogadó node kitölti
  doc["capabilities"] = JsonArray(); // ... ["temp","humi","battery"]
  doc["fw"]         = "2.6.0-mqtt";
  doc["ip"]         = WiFi.localIP().toString();
  String out; serializeJson(doc, out);
  mqtt.publish(topic("manifest").c_str(), out.c_str(), true);  // retained
  Serial.printf("[mqtt] manifest published: %s\n", out.c_str());
}

// ── Telemetry publish ──────────────────────────────────────
void publishSensor(const char* sensor, float value, const char* unit) {
  StaticJsonDocument<128> doc;
  doc["value"] = value;
  doc["unit"]  = unit;
  doc["ts"]    = millis();                    // node oldalon timestamp-eljük
  String out; serializeJson(doc, out);
  String t = topic("telemetry") + "/" + sensor;
  mqtt.publish(t.c_str(), out.c_str());
}

// ── A2AMessage envelope DM-küldéshez (agent_dm framing) ────
void sendDM(const char* from, const char* to, const char* text) {
  StaticJsonDocument<640> doc;
  doc["id"]       = String(ESP.getEfuseMac());  // unique-ish
  doc["type"]     = "a2a_message";
  doc["sender"]   = from;
  doc["recipient"] = to;
  doc["payload"]["chat_type"]     = "agent_dm";
  doc["payload"]["sender_display"] = DEVICE_NAME;
  doc["payload"]["text"]          = text;
  String out; serializeJson(doc, out);
  mqtt.publish(dmTopic(to).c_str(), out.c_str());
}

// ── Parancs-feldolgozás (a2a/devices/{id}/cmd/#) ───────────
void onCmd(char* topic_, byte* payload, unsigned int len) {
  StaticJsonDocument<256> doc;
  deserializeJson(doc, payload, len);
  const char* action = doc["action"] | "";
  Serial.printf("[cmd] %s: %s\n", topic_, action);
  // device-specifikus parancsok ide...
}

void setup() {
  Serial.begin(115200);
  WiFi.begin(WIFI_SSID, WIFI_PASS);
  while (WiFi.status() != WL_CONNECTED) delay(250);

  mqtt.setServer(MQTT_HOST, MQTT_PORT);
  mqtt.setCallback(onCmd);
  mqtt.setKeepAlive(30);

  // LWT: a broker automatikusan "offline"-t publikál ha a device kilép/lezuhan
  while (!mqtt.connect(DEVICE_ID, NULL, NULL,
                       topic("status").c_str(), 1, true, "offline")) {
    delay(1000);
  }
  mqtt.publish(topic("status").c_str(), "online", true);   // retained
  mqtt.subscribe(String(topic("cmd") + "/#").c_str());
  publishManifest();
}

unsigned long lastTelemetry = 0;
void loop() {
  mqtt.loop();
  // 10s telemetry példa:
  if (millis() - lastTelemetry > 10000) {
    lastTelemetry = millis();
    publishSensor("temp", 23.5 + random(-20, 20) / 10.0, "C");
    publishSensor("humi", 45.0 + random(-30, 30) / 10.0, "%");
    publishSensor("battery", 3.9, "V");
  }
}