!include ..\platform_test.nmakefile
TARGETS=sow.exe disconnect.exe exceptions.exe fix.exe functional.exe logon.exe ping.exe subscribe.exe publish_and_sow.exe amps_consume_queue.exe amps_fix_builder_publisher.exe amps_fix_shredder_subscriber.exe amps_nvfix_builder_publisher.exe amps_nvfix_shredder_subscriber.exe amps_publish_composite.exe amps_publish.exe amps_publish_for_replay.exe amps_publish_queue.exe amps_publish_sow.exe amps_query_sow.exe amps_sow_and_subscribe.exe amps_sow_and_subscribe_with_oof.exe amps_subscribe_composite.exe amps_subscribe.exe amps_subscribe_with_replay.exe amps_update_for_oof.exe
CFLAGS=/I..\include ..\lib\$(TARGET_PLATFORM)\Debug\amps.lib /MDd
CPPFLAGS=/Zi /EHsc /I..\include ..\lib\$(TARGET_PLATFORM)\Debug\amps.lib /MDd
all: $(TARGETS)

clean:
	-del $(TARGETS) *.pdb *.ilk *.obj 2>nul


