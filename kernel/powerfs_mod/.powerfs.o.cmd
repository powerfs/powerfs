savedcmd_powerfs.o := ld -m elf_x86_64 -z noexecstack --no-warn-rwx-segments   -r -o powerfs.o @powerfs.mod 
