#!/usr/bin/env python3
# DHAVinci by Jarle Thorsen (jarlethorsen@gmail.com)
import argparse
import logging
import mmap
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta

VERSION = 'v0.1'

logger = logging.getLogger(__file__)

class Formatter(logging.Formatter):
    def format(self, record):
        if record.levelno == logging.INFO:
            self._style._fmt = "* %(message)s"
        elif record.levelno == logging.DEBUG:
            self._style._fmt = "[%(name)s] (%(threadName)s) [%(module)s] %(funcName)s (%(filename)s:%(lineno)d) - %(message)s"
        else:
            self._style._fmt = "*%(levelname)s* - %(message)s"
        return super().format(record)

def init_argparse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        usage="%(prog)s <options> inputfile",
        description=f"DHAVinci {VERSION} - Carver for DHAV-video frames", epilog="Example: \"%(prog)s --csv --start 20240802154200 --stop 20240802164200 freespace.dd\""
    )
    parser.add_argument('inputfile', help='The file to search for DHAV-frames')
    parser.add_argument('-s', '--skip', type=int, nargs='?', default=0,
                    help='byte offset in file to start searching from')
    parser.add_argument('-o', '--output', type=str, nargs='?', default='',
                    help='directory to write output to, default is current directory')
    parser.add_argument('--start', type=str, nargs='?',
                    help='Only extract frames created after this timestamp. Timestamp should me provided in the following format: YYYYmmddhhmmss')
    parser.add_argument('--stop', type=str, nargs='?',
                    help='Only extract frames created before this timestamp. Timestamp should me provided in the following format: YYYYmmddhhmmss')
    parser.add_argument('--csv', action="store_true",
                    help='Write "found_all.csv" and "found_selection.csv" with header information for all frames')
    parser.add_argument('--dryrun', action="store_true",
                    help='Only search, do not extract any frames')
    parser.add_argument('-v', '--verbosity', help='Level of logging, -vv enables debug', action='count', default=0)
    return parser

@dataclass
class DHAVContext:
    type: int = 0
    subtype: int = 0
    channel: int = 0
    frame_subnumber: int = 0
    frame_number: int = 0
    date: int = 0
    timestamp: int = 0
    width: int = 0
    height: int = 0
    video_codec: int = 0
    frame_rate: int = 0
    audio_channels: int = 0
    audio_codec: int = 0
    sample_rate: int = 0
    last_good_pos: int = 0
    duration: int = 0
    video_stream_index: int = 0
    audio_stream_index: int = 0

    def read_data(self, f):
        self.signature = f.read(4)
        self.type = f.read(1)
        self.subtype = f.read(1)
        self.channel = int.from_bytes(f.read(1))
        self.frame_subnumber = int.from_bytes(f.read(1))
        self.frame_number = int.from_bytes(f.read(4), byteorder='little')
        self.frame_length = int.from_bytes(f.read(4), byteorder='little') # Number of bytes of this DHAV frame
        self.date = int.from_bytes(f.read(4), byteorder='little')
        self.timestamp = int.from_bytes(f.read(2), byteorder='little')
        
        # Go to start of frame and read the whole frame into self.data
        f.seek(-22, os.SEEK_CUR)
        self.data = f.read(self.frame_length)

def date_to_timestamp(date):
    sec   =   date        & 0x3F
    min   =  (date >>  6) & 0x3F
    hour  =  (date >> 12) & 0x1F
    day   =  (date >> 17) & 0x1F
    month =  (date >> 22) & 0x0F
    year  = ((date >> 26) & 0x3F) + 2000
    return datetime(year, month, day, hour, min, sec)

def str_to_timestamp(strtimestamp):
    return datetime.strptime(strtimestamp, '%Y%m%d%H%M%S')

def date_to_str(date):
    sec   =   date        & 0x3F
    min   =  (date >>  6) & 0x3F
    hour  =  (date >> 12) & 0x1F
    day   =  (date >> 17) & 0x1F
    month =  (date >> 22) & 0x0F
    year  = ((date >> 26) & 0x3F) + 2000
    return f'{year}{str(month).zfill(2)}{str(day).zfill(2)}{str(hour).zfill(2)}{str(min).zfill(2)}{str(sec).zfill(2)}'

def write_dav(outputfolder, frames):
    filename = f'NVR_{frames[0].channel}_main_{date_to_str(frames[0].date)}_{date_to_str(frames[-1].date)}.dav'
    output = os.path.join(outputfolder, filename)
    with open(output, 'wb') as f:
        for frame in frames:
            f.write(frame.data)

def timestamp_ok(timestamp, starttime, stoptime):
    if starttime and timestamp < starttime:
        return False
    if stoptime and timestamp > stoptime:
        return False
    return True

def main():
    # Parse args
    parser = init_argparse()
    args = vars(parser.parse_args(args=None if sys.argv[1:] else ['--help']))
    outputfolder = args.get('output')
    startoffset = args.get('skip')
    starttime = args.get('start')
    if starttime:
        starttime = str_to_timestamp(starttime)
    stoptime = args.get('stop')
    if stoptime:
        stoptime = str_to_timestamp(stoptime)
    if args.get('csv'):
        # Open csv files for writing
        alloutputfile = open(os.path.join(outputfolder, 'found_all.csv'), 'w')
        heading = 'timestamp,offset,type,subtype,channel,frame_number,frame_subnumber,frame_length,extra_timestamp'
        alloutputfile.write(f'{heading}\n')
        if starttime or stoptime:
            selectionoutputfile = open(os.path.join(outputfolder, 'found_selection.csv'), 'w')
            selectionoutputfile.write(f'{heading}\n')
    
    # Setup logging
    levels = [logging.WARNING, logging.INFO, logging.DEBUG]
    level = levels[min(len(levels) - 1, args.get('verbosity'))]  # capped to number of levels
    logger.setLevel(level)
    handler = logging.StreamHandler()
    handler.setFormatter(Formatter())
    logger.addHandler(handler)

    
    with open(args.get('inputfile'), 'r+b') as f:
        davoutputfolder = os.path.join(outputfolder, 'video')
        if not os.path.exists(davoutputfolder):
            os.makedirs(davoutputfolder)

        # startoffset needs to be dividable by mmap.ALLOCATIONGRANULARITY, so we subtract if needed
        over = startoffset % mmap.ALLOCATIONGRANULARITY
        startoffset -= over
        offset = 0
        # memory-map the file
        mm = mmap.mmap(f.fileno(), startoffset, access=mmap.ACCESS_READ)
        mapsize = mm.size()
        filesize = startoffset + mapsize
        frames = []
        
        # Start searching
        start_time = time.time()
        while True:
            header_offset = mm.find(b'DHAV', offset)
            if header_offset == -1:
                # No more headers found, write remaining frames to disk and exit
                if frames and timestamp_ok(timestamp, starttime, stoptime):
                    if not args.get('dryrun'):
                        write_dav(davoutputfolder, frames)
                break

            # Header found
            found_location = header_offset + startoffset
            found_time = time.time()
            running_time = found_time - start_time
            eta = 'N/A'
            if running_time > 0:
                speed = header_offset / running_time # bytes per second
            if speed > 0:
                remaining = (mapsize - header_offset) / speed # seconds remaining
                eta = str(timedelta(seconds=remaining)).split('.', 2)[0]
            logger.info(f'Found DHAV-frame at offset {found_location}/{filesize} ({int(found_location/filesize*100)}%) ETA:{eta}')
            if header_offset > offset and frames:
                # Found end of contiguous frames, write previous frames to disk, if within timeframe
                timestamp = date_to_timestamp(frames[0].date)
                if timestamp_ok(timestamp, starttime, stoptime):
                    if not args.get('dryrun'):
                        write_dav(davoutputfolder, frames)
                frames = []
            dhav = DHAVContext()
            mm.seek(header_offset, 0)
            dhav.read_data(mm)
            try:
                timestamp = date_to_timestamp(dhav.date)
            except ValueError:
                logger.debug(f'Illegal date timestamp {dhav.date} at offset {header_offset} DHAVContext: {dhav}')
            else:
                if args.get('csv'):
                    # Write frame-info to csv-file
                    if starttime or stoptime:
                        if timestamp_ok(timestamp, starttime, stoptime):
                            selectionoutputfile.write(f'{str(timestamp)},{header_offset+startoffset},{dhav.type.hex()},{dhav.subtype.hex()},{dhav.channel},{dhav.frame_number},{dhav.frame_subnumber},{dhav.frame_length},{dhav.timestamp}\n')
                    alloutputfile.write(f'{str(timestamp)},{header_offset+startoffset},{dhav.type.hex()},{dhav.subtype.hex()},{dhav.channel},{dhav.frame_number},{dhav.frame_subnumber},{dhav.frame_length},{dhav.timestamp}\n')

                frames.append(dhav)
            # Continue searching at end of frame
            offset = header_offset + dhav.frame_length
    
    if args.get('csv'):
        if selectionoutputfile:
            selectionoutputfile.close()
        if alloutputfile:
            alloutputfile.close()

if __name__ == '__main__':
    main()
