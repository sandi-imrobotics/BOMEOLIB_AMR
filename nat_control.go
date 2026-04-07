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
	Alarm         *Alarm  `json:"alarm"`
	CreatedAt     string  `json:"created_at"`
}

// report struct (facility_id instead of id)
type AMRReport struct {
	FacilityID    int     `json:"facility_id"`
	DriveMode     string  `json:"drive_mode"`
	CurrentAct    string  `json:"current_act_name"`
	PathSpotIDs   []int   `json:"path_spot_ids"`
	CurrentSpotID int     `json:"current_spot_id"`
	CurrentPose   Pose    `json:"current_pose"`
	IsRunning     bool    `json:"is_running"`
	IsCharging    bool    `json:"is_charging"`
	BatteryLevel  Battery `json:"battery_level"`
	Alarm         *Alarm  `json:"alarm"`
	CreatedAt     string  `json:"created_at"`
}

// ---------------- GLOBAL ----------------

var status AMRStatus
var lastHash string
var lastPublish time.Time
var lastReport time.Time

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

// ---------------- READY DELAY ----------------

func setReadyWithDelay() {
	go func() {
		time.Sleep(2 * time.Second)
		status.CurrentAct = "act.amr." + AMR_ID + ".ready"
	}()
}

// ---------------- UPDATE ----------------

func updateStatus(robot map[string]interface{}) {

	// ---------- POSE ----------
	if x, ok := robot["x"].(float64); ok {
		status.CurrentPose.X = x
	}
	if y, ok := robot["y"].(float64); ok {
		status.CurrentPose.Y = y
	}
	if a, ok := robot["angle"].(float64); ok {
		status.CurrentPose.Theta = a
	}

	// ---------- BATTERY ----------
	if bat, ok := robot["battery_level"].(float64); ok {
		status.BatteryLevel.AMR = int(bat * 100)
	}
	status.BatteryLevel.Kiosk = 100

	// ---------- DRIVE MODE (DI) ----------
	auto := false
	manual := false

	if di, ok := robot["DI"].([]interface{}); ok {
		for _, d := range di {
			if item, ok := d.(map[string]interface{}); ok {
				id, _ := item["id"].(float64)
				st, _ := item["status"].(bool)

				if int(id) == 4 {
					auto = st
				}
				if int(id) == 5 {
					manual = st
				}
			}
		}
	}

	prevMode := status.DriveMode

	if auto && !manual {
		status.DriveMode = "automatic"
	} else if !auto && manual {
		status.DriveMode = "manual"
	}

	// trigger on change automatic -> manual
	if prevMode == "automatic" && status.DriveMode == "manual" {
		status.CurrentAct = "act.amr." + AMR_ID + ".stop"
		setReadyWithDelay()
	}

	// ---------- EMERGENCY ----------
	if em, ok := robot["emergency"].(bool); ok && em {
		status.Alarm = &Alarm{"warn", "e-stop"}

		status.CurrentAct = "act.amr." + AMR_ID + ".stop"
		setReadyWithDelay()
	} else {
		status.Alarm = &Alarm{}
	}

	// ---------- PATH ----------
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

	// ---------- TASK ----------
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

func publishStatus(nc *nats.Conn) {
	status.CreatedAt = time.Now().Format("2006-01-02 15:04:05.000 -0700")
	data, _ := json.Marshal(status)
	nc.Publish("status.amr."+AMR_ID, data)
}

func publishReport(nc *nats.Conn) {
	report := AMRReport{
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
		CreatedAt:     time.Now().Format("2006-01-02 15:04:05.000 -0700"),
	}

	data, _ := json.Marshal(report)
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

// safe wrapper
func getSafeData() map[string]interface{} {
	data, err := getData(19204, 1100, map[string]interface{}{
		"return_laser":   false,
		"return_beams3D": false,
		"return_mid360":  false,
	})
	if err != nil {
		return map[string]interface{}{}
	}
	return data
}

// ---------------- ACT ----------------

type MoveCmd struct {
	ID           int `json:"id"`
	TargetSpotID int `json:"target_spot_id"`
}

func handleMove(msg *nats.Msg) {
	var cmd MoveCmd
	json.Unmarshal(msg.Data, &cmd)

	targetStr := fmt.Sprintf("LM%d", cmd.TargetSpotID)

	source := currentStation
	if source == "" {
		source = "SELF_POSITION"
	}

	getData(19206, 3051, map[string]interface{}{
		"source_id": source,
		"id":        targetStr,
	})

	status.CurrentAct = "act.amr." + AMR_ID + ".move"
	lastCommand = "move"
}

func handleCharge(msg *nats.Msg) {
	var cmd MoveCmd
	json.Unmarshal(msg.Data, &cmd)

	targetStr := fmt.Sprintf("LM%d", cmd.TargetSpotID)

	source := currentStation
	if source == "" {
		source = "SELF_POSITION"
	}

	getData(19206, 3051, map[string]interface{}{
		"source_id": source,
		"id":        targetStr,
	})

	status.CurrentAct = "act.amr." + AMR_ID + ".charge"
	lastCommand = "charge"
}

func handleStop(msg *nats.Msg) {
	getData(19206, 3003, map[string]interface{}{})
	status.CurrentAct = "act.amr." + AMR_ID + ".stop"
	lastCommand = ""
}

func handleReady(msg *nats.Msg) {
	getData(19206, 3003, map[string]interface{}{})
	status.CurrentAct = "act.amr." + AMR_ID + ".stop"
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
		Alarm:       &Alarm{},
		PathSpotIDs: []int{},
	}

	for i := 0; i < 200; i++ {
		getSafeData()
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
