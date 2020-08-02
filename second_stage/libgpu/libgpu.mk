OUTDIR=$(OBJDIR)/libgpu
$(OUTDIR): ; mkdir -p $@

DEPOUTDIR=$(DEPDIR)/libgpu
$(DEPOUTDIR): ; mkdir -p $@

FORCE: $(OUTDIR) $(DEPOUTDIR)

GPU_LIB = gpu_agg.c gpu_config.c gpu_query.c gpu_input_buffer.c gpu_output_buffer.c
GPU_LIB := $(foreach file,$(GPU_LIB),libgpu/$(file))
SRCS += $(GPU_LIB)

FORCE: $(echo $(GPU_LIB))
