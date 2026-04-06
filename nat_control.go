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
	FacilityID    int     `json:"id"`
	DriveMode     string  `json:"drive_mode"`
	CurrentAct    string  `json:"current_act_name"`
	PathSpotIDs   []int   `json:"path_spot_ids"`
	CurrentSpotID int     `json:"current_spot_id"`
	CurrentPose   Pose    `json:"current_pose"`
	IsRunning     bool    `json:"is_running"`
	IsCharging    bool    `json:"is_charging"`
	BatteryLevel  Battery `json:"battery_level"`
	SwitchMode    string  `json:"switch_mode"`
	Alarm         *Alarm  `json:"alarm"`
	CreatedAt     string  `json:"created_at"`
}

// ---------------- GLOBAL ----------------

var status AMRStatus
var lastHash string
var lastPublish time.Time
var lastCommand string = ""
var currentStation string = "SELF_POSITION"

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

// ---------------- PROTOCOL ----------------

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
		H1:      0x5A,
		H2:      0x01,
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
		return map[string]interface{}{"connected": false}, err
	}
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(5 * time.Second))

	packet, _ := packMsg(1, msgID, command)
	conn.Write(packet)

	headerBuf := make([]byte, 16)
	_, err = io.ReadFull(conn, headerBuf)
	if err != nil {
		return map[string]interface{}{"connected": false}, err
	}

	var header PacketHeader
	binary.Read(bytes.NewReader(headerBuf), binary.BigEndian, &header)

	body := make([]byte, header.MsgLen)
	if header.MsgLen > 0 {
		io.ReadFull(conn, body)
	}

	result := make(map[string]interface{})
	if len(body) > 0 {
		json.Unmarshal(body, &result)
	}

	result["connected"] = true
	return result, nil
}

// ---------------- ROBOT API ----------------

func queryBatchData() map[string]interface{} {
	data, err := getData(19204, 1100, map[string]interface{}{
		"return_laser":   false,
		"return_beams3D": false,
		"return_mid360":  false,
	})
	if err != nil {
		return map[string]interface{}{"connected": false}
	}
	return data
}

// ---------------- ALARM ----------------

func buildAlarm(robot map[string]interface{}) *Alarm {

	if fatals, ok := robot["fatals"].([]interface{}); ok && len(fatals) > 0 {
		if item, ok := fatals[0].(map[string]interface{}); ok {
			code, _ := item["code"].(float64)
			desc, _ := item["desc"].(string)
			return &Alarm{"error", fmt.Sprintf("%d %s", int(code), desc)}
		}
	}

	if errors, ok := robot["errors"].([]interface{}); ok && len(errors) > 0 {
		if item, ok := errors[0].(map[string]interface{}); ok {
			code, _ := item["code"].(float64)
			desc, _ := item["desc"].(string)
			return &Alarm{"error", fmt.Sprintf("%d %s", int(code), desc)}
		}
	}

	if status.BatteryLevel.Kiosk < 30 {
		return &Alarm{"critical", "kiosk battery low"}
	}
	if status.BatteryLevel.AMR < 30 {
		return &Alarm{"critical", "amr battery low"}
	}

	// return empty object instead of nil
	return &Alarm{}
}

// ---------------- READY DELAY ----------------

func setReadyWithDelay() {
	go func() {
		time.Sleep(2 * time.Second)
		status.CurrentAct = "act.amr." + AMR_ID + ".ready"
	}()
}

// ---------------- UPDATE ----------------

func updateStatus(robot map[string]interface{}) {

	if x, ok := robot["x"].(float64); ok {
		status.CurrentPose.X = x
	}
	if y, ok := robot["y"].(float64); ok {
		status.CurrentPose.Y = y
	}
	if a, ok := robot["angle"].(float64); ok {
		status.CurrentPose.Theta = a
	}

	if bat, ok := robot["battery_level"].(float64); ok {
		status.BatteryLevel.AMR = int(bat * 100)
	}

	status.BatteryLevel.Kiosk = 100

	if station, ok := robot["current_station"].(string); ok {
		if station != "" {
			currentStation = station
			status.CurrentSpotID = extractNumber(station)
		} else {
			currentStation = "SELF_POSITION"
		}
	}

	// ensure [] not null
	if path, ok := robot["unfinished_path"].([]interface{}); ok {
		result := []int{}
		for _, p := range path {
			if s, ok := p.(string); ok {
				result = append(result, extractNumber(s))
			}
		}
		status.PathSpotIDs = result
	} else {
		status.PathSpotIDs = []int{}
	}

	if ts, ok := robot["task_status"].(float64); ok {
		if int(ts) == 2 {
			status.IsRunning = true
		} else {
			status.IsRunning = false

			if lastCommand == "move" || lastCommand == "charge" {
				status.CurrentAct = "act.amr." + AMR_ID + ".stop"
				lastCommand = ""

				setReadyWithDelay()
			}
		}
	}

	status.Alarm = buildAlarm(robot)
}

// ---------------- HASH ----------------

func calcHash(s AMRStatus) string {
	tmp := s
	tmp.CreatedAt = ""
	data, _ := json.Marshal(tmp)
	hash := md5.Sum(data)
	return hex.EncodeToString(hash[:])
}

// ---------------- NATS ----------------

func publish(nc *nats.Conn) {
	status.CreatedAt = time.Now().Format("2006-01-02 15:04:05.000 -0700")
	data, _ := json.Marshal(status)
	nc.Publish("status.amr."+AMR_ID, data)
}

// ---------------- POLLING ----------------

func startPolling(nc *nats.Conn) {
	go func() {
		ticker := time.NewTicker(200 * time.Millisecond)
		defer ticker.Stop()

		for range ticker.C {
			robot := queryBatchData()
			updateStatus(robot)

			hash := calcHash(status)

			if hash != lastHash || time.Since(lastPublish) > time.Second {
				publish(nc)
				lastHash = hash
				lastPublish = time.Now()
			}
		}
	}()
}

// ---------------- ACT ----------------

type MoveCmd struct {
	ID           int `json:"id"`
	TargetSpotID int `json:"target_spot_id"`
}

func handleMove(msg *nats.Msg) {
	fmt.Println("RAW:", string(msg.Data))

	var cmd MoveCmd
	if err := json.Unmarshal(msg.Data, &cmd); err != nil {
		fmt.Println("JSON error:", err)
		return
	}

	targetStr := fmt.Sprintf("LM%d", cmd.TargetSpotID)

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

	var cmd MoveCmd
	if err := json.Unmarshal(msg.Data, &cmd); err != nil {
		fmt.Println("JSON error:", err)
		return
	}

	targetStr := fmt.Sprintf("LM%d", cmd.TargetSpotID)

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
	getData(19206, 3003, map[string]interface{}{})

	status.CurrentAct = "act.amr." + AMR_ID + ".stop"
	lastCommand = ""
}

func handleReady(msg *nats.Msg) {
	fmt.Println("READY COMMAND RECEIVED")

	getData(19206, 3003, map[string]interface{}{})

	status.CurrentAct = "act.amr." + AMR_ID + ".stop"
	lastCommand = ""

	setReadyWithDelay()
}

// ---------------- MAIN ----------------

func main() {

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
		Alarm:       &Alarm{}, // ensure {} at start
		PathSpotIDs: []int{},  // ensure [] at start
	}

	fmt.Println("Initializing AMR...")
	for i := 0; i < 500; i++ {
		queryBatchData()
		time.Sleep(10 * time.Millisecond)
	}

	nc.Subscribe("act.amr."+AMR_ID+".move", handleMove)
	nc.Subscribe("act.amr."+AMR_ID+".charge", handleCharge)
	nc.Subscribe("act.amr."+AMR_ID+".stop", handleStop)
	nc.Subscribe("act.amr."+AMR_ID+".ready", handleReady)

	nc.Publish("act.amr."+AMR_ID+".ready", []byte("{}"))

	startPolling(nc)

	fmt.Println("AMR READY")

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	<-sig
}
