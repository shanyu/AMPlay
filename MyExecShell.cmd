@echo start executing
rem TBD: not sure why calling powershell doesn't work on HDInsight cluster
rem      but it works fine on my onebox setup.
rem powershell start-sleep -s 30
ping /n 30 localhost
@echo end executing