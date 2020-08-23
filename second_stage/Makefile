# Compiler Configurations
CC        = gcc
CFLAGS    = -I$(shell pwd) -pthread
DBFLAGS   = -Wall -g
REFLAGS   = -O2
LDLIBS    = -lm
DEPFLAGS  = -MT $@ -MMD -MP -MF $(DEPDIR)/$*.d
COMPILE.c = $(CC) $(DEPFLAGS) $(CFLAGS) $(TARGET_ARCH) -c
LINK.o    = $(CC) $(CFLAGS) $(LDLIBS)
UNAME := $(shell uname -s)
ifeq ($(UNAME),Linux)
	CFLAGS += -I/usr/local/cuda/include
	LDLIBS += -lOpenCL
else
ifeq ($(UNAME),Darwin)
	LDLIBS += -framework OpenCL
endif
endif

# Environmrnt
OBJDIR  := obj
DEPDIR  := $(OBJDIR)/.deps
LIBDIR  :=

# Targets
PROGS   = test_gcd
SRCS = test_gcd.c config.c helpers.c schema.c query.c batch.c window.c generators.c task.c dispatcher.c

# Scripts
$(DEPDIR): ; mkdir -p $@
$(OBJDIR): ; mkdir -p $@

# Submodules
include ./cirbuf/cirbuf.mk
include ./libgpu/libgpu.mk
include ./operators/operators.mk
include ./monitor/monitor.mk
include ./scheduler/scheduler.mk

OBJECTS = $(patsubst %.c,%.o,$(SRCS))

# Compile objects with auto dependency
$(OBJDIR)/%.o : %.c $(DEPDIR)/%.d | $(OBJDIR) $(DEPDIR) $(LIBDIR)
		$(COMPILE.c) $(OUTPUT_OPTION) $<

DEPFILES := $(SRCS:%.c=$(DEPDIR)/%.d)
$(DEPFILES):
include $(wildcard $(DEPFILES))


# Compile the main program
OBJPROG = $(addprefix $(OBJDIR)/, $(PROGS))

all: CFLAGS += $(REFLAGS)
all: $(OBJPROG)

debug: CFLAGS += $(DBFLAGS)
debug: $(OBJPROG)

$(OBJPROG): $(addprefix $(OBJDIR)/, $(OBJECTS)) 
		$(LINK.o) $^ $(LDLIBS) -o $@


# clean
.PHONY: clean
clean:
	rm -f $(OBJDIR)/**/*.o $(DEPDIR)/**/*.d $(OBJPROG) *~
