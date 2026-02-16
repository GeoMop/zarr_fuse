FILE="$1/test_file"
dd if=/dev/zero of="$FILE" bs=64M count=16 oflag=direct status=progress

dd if="$FILE" of=/dev/null bs=64M iflag=direct status=progress

rm -f "$FILE"
