gen:
	mockery -name Dumper \
    		-case underscore \
    		-dir . \
    		-outpkg ballistic -output .