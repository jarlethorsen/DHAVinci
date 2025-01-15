# DHAVinci
Video-recovery tool for DHAV video frames

    $ python DHAVinci.py 
    usage: DHAVinci.py <options> inputfile
    
    DHAVinci v0.2 - Carver for DHAV-video frames
    
    positional arguments:
      inputfile             The file to search for DHAV-frames
    
    options:
      -h, --help            show this help message and exit
      -s [SKIP], --skip [SKIP]
                            byte offset in file to start searching from
      -o [OUTPUT], --output [OUTPUT]
                            directory to write output to, default is current directory
      --start [START]       Only extract frames created after this timestamp. Timestamp should me provided in the following format: YYYYmmddhhmmss
      --stop [STOP]         Only extract frames created before this timestamp. Timestamp should me provided in the following format: YYYYmmddhhmmss
      --csv                 Write "dhav.csv" with header information for all frames
      --dryrun              Only search, do not extract any frames
      --h264                Only write h264 streams, without DHAV-container
      -v, --verbosity       Level of logging, -vv enables debug
    
    Example: "DHAVinci.py --csv --start 20240802154200 --stop 20240802164200 freespace.dd"
