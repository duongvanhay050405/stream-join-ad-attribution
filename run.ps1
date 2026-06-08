Write-Host "Building project..." -ForegroundColor Cyan
mvn clean package -DskipTests

Write-Host "`nRunning Flink Job..." -ForegroundColor Cyan
java --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.lang.reflect=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED --add-opens java.base/java.io=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED --add-opens java.base/java.nio=ALL-UNNAMED -cp target\stream-join-1.0.jar com.join.StreamJoinJob