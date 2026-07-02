KERNELDIR ?= /lib/modules/$(shell uname -r)/build
PWD := $(shell pwd)
MODULE_NAME := powerfs

obj-m += $(MODULE_NAME).o
powerfs-objs := powerfs_mod.o powerfs_fs.o powerfs_transport.o

all:
	$(MAKE) -C $(KERNELDIR) M=$(PWD) modules

clean:
	$(MAKE) -C $(KERNELDIR) M=$(PWD) clean

install:
	$(MAKE) -C $(KERNELDIR) M=$(PWD) modules_install
	depmod -a

load:
	insmod $(MODULE_NAME).ko

unload:
	rmmod $(MODULE_NAME)

test:
	dmesg | tail -20
