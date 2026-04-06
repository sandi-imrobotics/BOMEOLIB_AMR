@echo off

cd /d C:\Users\iMR\Documents\IMR\NATS_CONTROL

echo Waiting for NATS server (port 4222)...

:wait_loop
netstat -an | find "4222" | find "LISTENING" >nul
if errorlevel 1 (
    echo NATS not ready, retrying...
    timeout /t 2 >nul
    goto wait_loop
)

echo NATS is ready!

start "" go run nat_control.go