#############make file for tangtest###################
PLATFORM=$(shell uname)

ifeq ($(PLATFORM),Linux)
	CXX=gcc -g -Wall -D_REENTRANT -D_GNU_SOURCE
	MAKE=make
	LIBS=-lpthread -lz -L/usr/lib
	CXXFLAGS=-DLinux
endif


ifeq ($(PLATFORM),SunOS)
	CXX=cc -g -D_REENTRANT -D_POSIX_PTHREAD_SEMANTICS -D_GNU_SOURCE
	CXXFLAGS=-DSunOS
	LIBS=-lpthread -lsocket -lnsl -lz -lposix4 -ldl -lrt
endif


#application directory####
APPPATH:=$(shell pwd)

VPATH=.

INCLUDES=-I/usr/vacpp/include

ALL_OBJS=qpipe.o


#生成应用#
all: qpipe 

qpipe:$(ALL_OBJS)
	$(CXX) -o qpipe  $(ALL_OBJS) $(LIBS)

clean: 
	rm qpipe.o;rm qpipe

#################################################################
.SUFFIXES: .c .o 
.c.o:
	@echo Compile file $<,`more $<|wc -l` lines ....
	$(CXX) $(INCLUDES) -c $< -o $*.o
################################################################
#################makefile end################################
