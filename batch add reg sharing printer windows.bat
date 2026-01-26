reg add "HKLM\System\CurrentControlSet\Control\Print" /v RpcAuthnLevelPrivacyEnabled /t REG_DWORD /d 0
reg add "HKLM\Software\Policies\Microsoft\Windows NT\Printers\RPC" /v RpcUseNamedPipeProtocol /t REG_DWORD /d 1
reg add "HKLM\Software\Policies\Microsoft\Windows NT\Printers\RPC" /v RpcOverTcp /t REG_DWORD /d 0
reg add "HKLM\Software\Policies\Microsoft\Windows NT\Printers\RPC" /v RpcOverNamedPipes /t REG_DWORD /d 1