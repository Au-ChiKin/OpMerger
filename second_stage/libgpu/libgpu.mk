GPU_OBJDIR=$(OBJDIR)/libgpu
$(GPU_OBJDIR): ; mkdir -p $@

GPU_DEPDIR=$(DEPDIR)/libgpu
$(GPU_DEPDIR): ; mkdir -p $@

GPU_LIB = gpu_agg.c gpu_config.c gpu_query.c gpu_input_buffer.c gpu_output_buffer.c openclerrorcode.c
GPU_LIB := $(foreach file,$(GPU_LIB),libgpu/$(file))
SRCS += $(GPU_LIB)

LIBDIR += $(GPU_DEPDIR) $(GPU_OBJDIR)