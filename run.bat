@echo off
echo 正在编译项目...
call mvn clean package -DskipTests
if %errorlevel% neq 0 (
    echo 编译失败
    pause
    exit /b %errorlevel%
)

echo.
echo 正在启动RTSP转HLS流媒体服务...
java -jar target\rtsp-stream-1.0.0.jar

pause
