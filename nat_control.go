package main

import (
	"bytes"
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"time"

	"github.com/nats-io/nats.go"
)

// ---------------- CONFIG ----------------
const (
	AMR_IP = "192.168.192.5"
	AMR_ID = "01"

	NATS_URL  = "nats://127.0.0.1:4222"
	NATS_USER = "nats"
	NATS_PASS = "00000000"
)

// ---------------- LOGGER ----------------

type LogEntry struct {
	Time    string `json:"time"`
	Level   string `json:"level"`
	Event   string `json:"event"`
	Message string `json:"message"`
}

var logFile *os.File
var errFile *os.File
var currentLogDate string

func initLogger() {
	os.MkdirAll("logs", os.ModePerm)
	openLogFiles()
	cleanupOldLogs()
}

func openLogFiles() {
	today := time.Now().Format("2006-01-02")

	if currentLogDate == today && logFile != nil && errFile != nil {
		return
	}

	if logFile != nil {
		logFile.Close()
	}
	if errFile != nil {
		errFile.Close()
	}

	logName := fmt.Sprintf("logs/%s.log", today)
	errName := fmt.Sprintf("logs/%s.error.log", today)

	var err error
	logFile, err = os.OpenFile(logName, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}

	errFile, err = os.OpenFile(errName, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}

	currentLogDate = today
}

func cleanupOldLogs() {
	files, _ := os.ReadDir("logs")
	cutoff := time.Now().AddDate(0, 0, -90)

	for _, f := range files {
		if f.IsDir() {
			continue
		}
		t, err := time.Parse("2006-01-02.log", f.Name())
		if err != nil {
			continue
		}
		if t.Before(cutoff) {
			os.Remove("logs/" + f.Name())
		}
	}
}

func logJSON(level, event, message string) {
	openLogFiles()

	entry := LogEntry{
		Time:    time.Now().Format("2006-01-02 15:04:05.000"),
		Level:   level,
		Event:   event,
		Message: message,
	}

	data, _ := json.Marshal(entry)
	line := string(data) + "\n"

	logFile.WriteString(line)
	fmt.Print(line)

	if level == "ERROR" {
		errFile.WriteString(line)
	}
}

func logInfo(e, m string)  { logJSON("INFO", e, m) }
func logWarn(e, m string)  { logJSON("WARN", e, m) }
func logError(e, m string) { logJSON("ERROR", e, m) }

// ---------------- STRUCTS ----------------

type Pose struct {
	X     float64 `json:"x"`
	Y     float64 `json:"y"`
	Theta float64 `json:"theta"`
}

type Battery struct {
	Kiosk int `json:"kiosk"`
	AMR   int `json:"amr"`
}

type Alarm struct {
	Type   string `json:"level"`
	Reason string `json:"reason"`
}

type AMRStatus struct {
	FacilityID    int      `json:"id"`
	DriveMode     string   `json:"drive_mode"`
	CurrentAct    string   `json:"current_act_name"`
	PathSpotIDs   []string `json:"path_spot_names"`
	CurrentSpotID string   `json:"current_spot_name"`
	CurrentPose   Pose     `json:"current_pose"`
	IsRunning     bool     `json:"is_running"`
	IsCharging    bool     `json:"is_charging"`
	BatteryLevel  Battery  `json:"battery_level"`
	Alarm         *Alarm   `json:"alarm"`
	CreatedAt     string   `json:"created_at"`
}

type AMRReport struct {
	FacilityID    int      `json:"facility_id"`
	DriveMode     string   `json:"drive_mode"`
	CurrentAct    string   `json:"current_act_name"`
	PathSpotIDs   []string `json:"path_spot_names"`
	CurrentSpotID string   `json:"current_spot_name"`
	CurrentPose   Pose     `json:"current_pose"`
	IsRunning     bool     `json:"is_running"`
	IsCharging    bool     `json:"is_charging"`
	BatteryLevel  Battery  `json:"battery_level"`
	Alarm         *Alarm   `json:"alarm"`
	CreatedAt     string   `json:"created_at"`
}

// ---------------- GLOBAL ----------------

var status AMRStatus
var lastHash string
var lastPublish time.Time
var lastReport time.Time

var lastCommand string
var currentStation = "SELF_POSITION"

var prevAlarm string
var prevAct string
var prevDriveMode string
var prevEmergency bool

var blockStartTime time.Time
var blockActive bool

// ---------------- UTIL ----------------

var reNum = regexp.MustCompile(`\d+`)

func extractNumber(s string) int {
	match := reNum.FindString(s)
	if match == "" {
		return 0
	}
	var val int
	fmt.Sscanf(match, "%d", &val)
	return val
}

// ---------------- TCP ----------------

type PacketHeader struct {
	H1      byte
	H2      byte
	ReqID   uint16
	MsgLen  uint32
	MsgType uint16
	Reserve [6]byte
}

func packMsg(reqID uint16, msgType uint16, msg map[string]interface{}) ([]byte, error) {
	jsonData, _ := json.Marshal(msg)

	header := PacketHeader{
		H1: 0x5A, H2: 0x01,
		ReqID:   reqID,
		MsgLen:  uint32(len(jsonData)),
		MsgType: msgType,
	}

	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, header)
	buf.Write(jsonData)

	return buf.Bytes(), nil
}

func getData(port int, msgID uint16, command map[string]interface{}) (map[string]interface{}, error) {

	conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", AMR_IP, port), 3*time.Second)
	if err != nil {
		logError("TCP_CONNECT_FAIL", err.Error())
		return map[string]interface{}{}, err
	}
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(5 * time.Second))

	packet, _ := packMsg(1, msgID, command)
	conn.Write(packet)

	headerBuf := make([]byte, 16)
	_, err = io.ReadFull(conn, headerBuf)
	if err != nil {
		logError("TCP_READ_FAIL", err.Error())
		return map[string]interface{}{}, err
	}

	var header PacketHeader
	binary.Read(bytes.NewReader(headerBuf), binary.BigEndian, &header)

	body := make([]byte, header.MsgLen)
	if header.MsgLen > 0 {
		io.ReadFull(conn, body)
	}

	result := map[string]interface{}{}
	json.Unmarshal(body, &result)

	return result, nil
}

// ---------------- READY ----------------

func setReadyWithDelay() {
	go func() {
		time.Sleep(2 * time.Second)
		status.CurrentAct = "act.amr." + AMR_ID + ".ready"
	}()
}

// ---------------- UPDATE ----------------

func updateStatus(robot map[string]interface{}) {

	// pose
	status.CurrentPose.X, _ = robot["x"].(float64)
	status.CurrentPose.Y, _ = robot["y"].(float64)
	status.CurrentPose.Theta, _ = robot["angle"].(float64)

	// battery
	if bat, ok := robot["battery_level"].(float64); ok {
		status.BatteryLevel.AMR = int(bat * 100)
	}
	status.BatteryLevel.Kiosk = 100

	// drive mode
	auto, manual := false, false
	if di, ok := robot["DI"].([]interface{}); ok {
		for _, d := range di {
			m := d.(map[string]interface{})
			id := int(m["id"].(float64))
			st := m["status"].(bool)
			if id == 4 {
				auto = st
			}
			if id == 5 {
				manual = st
			}
		}
	}

	if auto && !manual {
		status.DriveMode = "automatic"
	} else if !auto && manual {
		status.DriveMode = "manual"
	}

	if prevDriveMode != "" && prevDriveMode != status.DriveMode {
		logInfo("DRIVE_MODE_CHANGE", prevDriveMode+" to "+status.DriveMode)

		if prevDriveMode == "automatic" && status.DriveMode == "manual" {
			getData(19206, 3003, map[string]interface{}{})
			status.CurrentAct = "act.amr." + AMR_ID + ".stop"
			setReadyWithDelay()
		}
	}
	prevDriveMode = status.DriveMode

	// emergency
	em, _ := robot["emergency"].(bool)
	if em && !prevEmergency {
		logWarn("EMERGENCY", "emergency stop pressed")
		status.CurrentAct = "act.amr." + AMR_ID + ".stop"
		setReadyWithDelay()
	}
	prevEmergency = em

	// charging
	status.IsCharging, _ = robot["charging"].(bool)

	// CurrentSpotID
	if station, ok := robot["current_station"].(string); ok {
		if station != "" {
			currentStation = station
			status.CurrentSpotID = strconv.Itoa(extractNumber(station))
		} else {
			currentStation = "SELF_POSITION"
		}
	}

	// path
	status.PathSpotIDs = []string{}
	if path, ok := robot["unfinished_path"].([]interface{}); ok {
		for _, p := range path {
			status.PathSpotIDs = append(status.PathSpotIDs, strconv.Itoa(extractNumber(p.(string))))
		}
	}

	// task
	if ts, ok := robot["task_status"].(float64); ok {
		if int(ts) == 2 {
			status.IsRunning = true
		} else {
			status.IsRunning = false
			if lastCommand != "" {
				status.CurrentAct = "act.amr." + AMR_ID + ".stop"
				lastCommand = ""
				setReadyWithDelay()
			}
		}
	}

	status.Alarm = buildAlarm(robot)

	// log alarm change
	alarmStr, _ := json.Marshal(status.Alarm)
	if string(alarmStr) != prevAlarm && string(alarmStr) != "{}" {
		logWarn("ALARM", string(alarmStr))
		prevAlarm = string(alarmStr)
	}

	// log act change
	if status.CurrentAct != prevAct {
		logInfo("ACT_CHANGE", status.CurrentAct)
		prevAct = status.CurrentAct
	}
}

// ---------------- NATS ----------------

func publishStatus(nc *nats.Conn) {
	status.CreatedAt = time.Now().Format("2006-01-02 15:04:05.000 -07:00")
	data, _ := json.Marshal(status)
	nc.Publish("status.amr."+AMR_ID, data)
}

func publishReport(nc *nats.Conn) {
	r := AMRReport{
		FacilityID:    status.FacilityID,
		DriveMode:     status.DriveMode,
		CurrentAct:    status.CurrentAct,
		PathSpotIDs:   status.PathSpotIDs,
		CurrentSpotID: status.CurrentSpotID,
		CurrentPose:   status.CurrentPose,
		IsRunning:     status.IsRunning,
		IsCharging:    status.IsCharging,
		BatteryLevel:  status.BatteryLevel,
		Alarm:         status.Alarm,
		CreatedAt:     time.Now().Format("2006-01-02 15:04:05.000 -07:00"),
	}
	data, _ := json.Marshal(r)
	nc.Publish("report.amr."+AMR_ID, data)
}

// ---------------- POLLING ----------------

func startPolling(nc *nats.Conn) {
	go func() {
		ticker := time.NewTicker(200 * time.Millisecond)
		defer ticker.Stop()

		for range ticker.C {
			robot := getSafeData()
			updateStatus(robot)

			hash := calcHash(status)

			if hash != lastHash || time.Since(lastPublish) > time.Second {
				publishStatus(nc)
				lastHash = hash
				lastPublish = time.Now()
			}

			if time.Since(lastReport) > time.Second {
				publishReport(nc)
				lastReport = time.Now()
			}
		}
	}()
}

func getSafeData() map[string]interface{} {
	data, _ := getData(19204, 1100, map[string]interface{}{})
	return data
}

// ---------------- HASH ----------------

func calcHash(s AMRStatus) string {
	tmp := s
	tmp.CreatedAt = ""
	data, _ := json.Marshal(tmp)
	hash := md5.Sum(data)
	return hex.EncodeToString(hash[:])
}

// ---------------- ALARM ----------------

func buildAlarm(robot map[string]interface{}) *Alarm {

	if fatals, ok := robot["fatals"].([]interface{}); ok && len(fatals) > 0 {
		if item, ok := fatals[0].(map[string]interface{}); ok {
			code, _ := item["code"].(float64)
			desc, _ := item["desc"].(string)
			return &Alarm{"critical", fmt.Sprintf("%d %s", int(code), desc)}
		}
	}

	if errors, ok := robot["errors"].([]interface{}); ok && len(errors) > 0 {
		if item, ok := errors[0].(map[string]interface{}); ok {
			code, _ := item["code"].(float64)
			desc, _ := item["desc"].(string)
			if int(code) != 52200 {
				return &Alarm{"critical", fmt.Sprintf("%d %s", int(code), desc)}
			}
		}
	}

	emc, _ := robot["emergency"].(bool)
	if emc {
		return &Alarm{"critical", "emergency stop pressed"}
	}

	if status.BatteryLevel.Kiosk < 20 {
		return &Alarm{"critical", "kiosk battery low"}
	}
	if status.BatteryLevel.AMR < 20 {
		return &Alarm{"critical", "amr battery low"}
	}

	// ---------- BLOCK HANDLING ----------
	blk, _ := robot["blocked"].(bool)

	if blk {

		// first time block detected
		if !blockActive {
			blockStartTime = time.Now()
			blockActive = true
		}

		elapsed := time.Since(blockStartTime).Seconds()

		if elapsed < 10 {
			return &Alarm{"noti", "blocked <10s"}
		} else if elapsed < 20 {
			return &Alarm{"warn", "blocked 10-20s"}
		} else {
			return &Alarm{"critical", "blocked >20s"}
		}

	} else {
		// reset when not blocked
		blockActive = false
	}

	if status.BatteryLevel.Kiosk < 30 {
		return &Alarm{"warn", "kiosk battery low"}
	}
	if status.BatteryLevel.AMR < 30 {
		return &Alarm{"warn", "amr battery low"}
	}

	return &Alarm{"", ""}
}

// ---------------- ACT ----------------

type MoveCmd struct {
	ID           int    `json:"id"`
	TargetSpotID string `json:"target_spot_name"`
}

func handleMove(msg *nats.Msg) {
	fmt.Println("RAW:", string(msg.Data))
	logInfo("MOVE", string(msg.Data))

	var cmd MoveCmd
	if err := json.Unmarshal(msg.Data, &cmd); err != nil {
		fmt.Println("JSON error:", err)
		return
	}

	targetStr := "LM" + cmd.TargetSpotID

	source := currentStation
	if source == "" {
		source = "SELF_POSITION"
	}

	moveCmd := map[string]interface{}{
		"source_id": source,
		"id":        targetStr,
	}

	fmt.Println("MOVE CMD:", moveCmd)

	_, err := getData(19206, 3051, moveCmd)
	if err != nil {
		fmt.Println("MOVE API error:", err)
		return
	}

	status.CurrentAct = "act.amr." + AMR_ID + ".move"
	lastCommand = "move"
}

func handleCharge(msg *nats.Msg) {
	fmt.Println("RAW:", string(msg.Data))
	logInfo("CHARGE", string(msg.Data))

	var cmd MoveCmd
	if err := json.Unmarshal(msg.Data, &cmd); err != nil {
		fmt.Println("JSON error:", err)
		return
	}

	targetStr := "LM" + cmd.TargetSpotID

	source := currentStation
	if source == "" {
		source = "SELF_POSITION"
	}

	moveCmd := map[string]interface{}{
		"source_id": source,
		"id":        targetStr,
	}

	fmt.Println("CHARGE CMD:", moveCmd)

	getData(19206, 3051, moveCmd)

	status.CurrentAct = "act.amr." + AMR_ID + ".charge"
	lastCommand = "charge"
}

func handleStop(msg *nats.Msg) {
	fmt.Println("STOP")
	logInfo("STOP", string(msg.Data))
	getData(19206, 3003, map[string]interface{}{})

	status.CurrentAct = "act.amr." + AMR_ID + ".stop"
	lastCommand = ""
}

func handleReady(msg *nats.Msg) {
	fmt.Println("READY COMMAND RECEIVED")
	logInfo("READY", string(msg.Data))
	getData(19206, 3003, map[string]interface{}{})

	status.CurrentAct = "act.amr." + AMR_ID + ".stop"
	lastCommand = ""

	setReadyWithDelay()
}

// ---------------- MAIN ----------------

func main() {

	initLogger()

	nc, err := nats.Connect(
		NATS_URL,
		nats.UserInfo(NATS_USER, NATS_PASS),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(2*time.Second),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()

	status = AMRStatus{
		FacilityID:  1,
		DriveMode:   "automatic",
		CurrentAct:  "booting",
		Alarm:       &Alarm{},
		PathSpotIDs: []string{}, // ensure [] at start
	}

	fmt.Println("Initializing AMR...")
	for i := 0; i < 100; i++ {
		getSafeData()
		time.Sleep(10 * time.Millisecond)
	}

	nc.Subscribe("act.amr."+AMR_ID+".move", handleMove)
	nc.Subscribe("act.amr."+AMR_ID+".charge", handleCharge)
	nc.Subscribe("act.amr."+AMR_ID+".stop", handleStop)
	nc.Subscribe("act.amr."+AMR_ID+".ready", handleReady)

	// trigger ready via NATS
	nc.Publish("act.amr."+AMR_ID+".ready", []byte("{}"))

	startPolling(nc)

	fmt.Println("AMR READY")

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	<-sig
}
