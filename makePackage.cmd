set OUT_FOLDER=dist
set CLIENT_JAR_PATH=AMPlayClient\target
set CLIENT_JAR_NAME=AMPlayClient
set MASTER_JAR_PATH=AMPlayMaster\target
set MASTER_JAR_NAME=AMPlayMaster

call rmdir /S /Q %OUT_FOLDER%
call mkdir %OUT_FOLDER%\%CLIENT_JAR_PATH%
call mkdir %OUT_FOLDER%\%MASTER_JAR_PATH%
call copy %CLIENT_JAR_PATH%\%CLIENT_JAR_NAME%*.jar %OUT_FOLDER%\%CLIENT_JAR_PATH%
call copy %MASTER_JAR_PATH%\%MASTER_JAR_NAME%*.jar %OUT_FOLDER%\%MASTER_JAR_PATH%
call copy runJob.cmd %OUT_FOLDER%\
call copy copyToHdfs.cmd %OUT_FOLDER%\
call copy MyExecShell.cmd %OUT_FOLDER%\