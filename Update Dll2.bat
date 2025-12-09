@Echo Off
@For /F "tokens=1,2,3,4 delims=/ " %%A in ('Date /t') do @(
Set DayW=%%A
Set Day=%%B
Set Month=%%C
Set Year=%%D
Set All=%%D-%%B-%%C
)
@For /F "tokens=1,2,3 delims=:,. " %%A in ('echo %time%') do @(
Set Hour=%%A
Set Min=%%B
Set Sec=%%C
Set Allm=%%A.%%B.%%C
)
@For /F "tokens=3 delims=: " %%A in ('time /t ') do @(
Set AMPM=%%A
)

:AWAL
cls
set /p name= Enter Dll name :

Echo Update dll %name%


REN \\10.78.100.54\appzwat$\HRTM\%name%.dll %name%_%All%_%Allm%_%AMPM%.dll
DEL \\10.78.100.54\appzwat$\HRTM\%name%_%All%_%Allm%_%AMPM%.dll
COPY \\10.78.100.54\appzmm$\Common\UpdateBat\HRTM\%name%.dll \\10.78.100.54\appzwat$\HRTM\%name%.dll
ECHO 'WAT'

REN \\10.78.100.54\appzjkt$\HRTM\%name%.dll %name%_%All%_%Allm%_%AMPM%.dll
COPY \\10.78.100.54\appzmm$\Common\UpdateBat\HRTM\%name%.dll \\10.78.100.54\appzjkt$\HRTM\%name%.dll
ECHO 'JKT'

REN \\10.78.100.54\appzmoni$\HRTM\%name%.dll %name%_%All%_%Allm%_%AMPM%.dll
DEL \\10.78.100.54\appzmoni$\HRTM\%name%_%All%_%Allm%_%AMPM%.dll
COPY \\10.78.100.54\appzmm$\Common\UpdateBat\HRTM\%name%.dll \\10.78.100.54\appzmoni$\HRTM\%name%.dll
ECHO 'MONI'

Echo 'backup ke mes'
copy \\10.78.100.54\appzjkt$\HRTM\%name%_%All%_%Allm%_%AMPM%.dll \\10.78.100.54\appzmm$\Common\AppzBK\HRTM\%name%_%All%_%Allm%_%AMPM%.dll
DEL \\10.78.100.54\appzjkt$\HRTM\%name%_%All%_%Allm%_%AMPM%.dll
Echo 'Done"


::PAUSE
:PROMPT_AGAIN
set "again="
set /p again="Do you want to update another file (Y/N): "
if /I "%again%"=="Y" (
	call :AWAL
)
if /I "%again%"=="N" exit

echo Please answer Y or N.
goto PROMPT_AGAIN

PAUSE