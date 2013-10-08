call hadoop fs -rm -R /amplay
call hadoop fs -mkdir /amplay
call hadoop fs -copyFromLocal AMPlayMaster/target/AMPlayMaster*.jar /amplay/
call hadoop fs -copyFromLocal MyExecShell.cmd /amplay/