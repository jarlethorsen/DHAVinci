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
frametypes = set()

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
                    help='Write "dhav.csv" with header information for all frames')
    parser.add_argument('--dryrun', action="store_true",
                    help='Only search, do not extract any frames')
    parser.add_argument('--h264', action="store_true",
                    help='Only write h264 streams, without DHAV-container')
    parser.add_argument('-v', '--verbosity', help='Level of logging, -vv enables debug', action='count', default=0)
    return parser

@dataclass
class DHAVContext:
    MAX_FRAME_LENGTH: int = 4_227_858_432
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
        self.type = f.read(1) # 0xf0, 0xf1, 0xfc, 0xfd
        self.subtype = f.read(1)
        self.channel = int.from_bytes(f.read(1))
        self.frame_subnumber = int.from_bytes(f.read(1))
        self.frame_number = int.from_bytes(f.read(4), byteorder='little')
        self.frame_length = int.from_bytes(f.read(4), byteorder='little') # Number of bytes of this DHAV frame
        self.date = int.from_bytes(f.read(4), byteorder='little')
        self.timestamp = int.from_bytes(f.read(2), byteorder='little')
      
        # Go to start of frame and read the whole frame into self.data
        f.seek(-22, os.SEEK_CUR)
        if self.frame_length > self.MAX_FRAME_LENGTH:
            logger.error(f'Found {self.type.hex()} frame at offset {f.tell()} with a length of {self.frame_length} This is WAY to big! This frame should probably be skipped')
            # self.frame_length = self.MAX_FRAME_LENGTH
        self.data = f.read(self.frame_length)
    
    def type_ok(self):
        """
        Frame Type Summary Table (according to ChatGPT)
        Hex Code	Frame Type	Description
        0xFC	I-Frame	Complete image, independent.
        0xFD	P-Frame	Encodes differences from previous frame.
        0xFE	Audio Frame	Encoded audio data (e.g., G.711).
        0xFA	Metadata Frame	Contains timestamps, events, or camera metadata.
        0xF0    Metatada
        0xF1    Typically contains extended metadata, event markers, or auxiliary information.
        0xFB	Metadata Frame	Additional metadata (e.g., motion detection).
        0x00	Padding/Error Frame	Filler or error indicator.
        """
        if self.type in [b'\xfc', b'\xfd', b'\xf0', b'\xf1']:
            return True
        else:
            logger.debug(f'Found unknown DHAV-type: {self.type}')

def date_to_timestamp(date):
    strtimestamp = date_to_str(date)
    return str_to_timestamp(strtimestamp)

def str_to_timestamp(strtimestamp):
    return datetime.strptime(strtimestamp, '%Y%m%d%H%M%S')

def date_to_str(date):
    """
    Example, assuming date == 1658512628:
    01100010110110101110010011110100
    year  month day   hour  min    sec 
    011000 1011 01101 01110 010011 110100
    """
    sec   =   date        & 0x3F
    min   =  (date >>  6) & 0x3F
    hour  =  (date >> 12) & 0x1F
    day   =  (date >> 17) & 0x1F
    month =  (date >> 22) & 0x0F
    year  = ((date >> 26) & 0x3F) + 2000
    return f'{year}{str(month).zfill(2)}{str(day).zfill(2)}{str(hour).zfill(2)}{str(min).zfill(2)}{str(sec).zfill(2)}'

def write_dav(outputfolder, frames):
    filename = f'NVR_{frames[0][1].channel}_main_{date_to_str(frames[0][1].date)}_{date_to_str(frames[-1][1].date)}.dav'
    output = os.path.join(outputfolder, filename)
    with open(output, 'wb') as f:
        for _, frame in frames:
            f.write(frame.data)

def write_h264(outputfolder, frames):
    filename = f'NVR_{frames[0][1].channel}_main_{date_to_str(frames[0][1].date)}_{date_to_str(frames[-1][1].date)}.h264'
    output = os.path.join(outputfolder, filename)
    with open(output, 'wb') as f:
        for _, frame in frames:
            if frame.data[40:44] != b'\x00\x00\x00\x01':
                logger.warning('No h264 start signature found.')
            f.write(frame.data[40:])

def write_csv(fh, frames):
    for offset, dhav in frames:
        fh.write(f'{str(date_to_timestamp(dhav.date))},{offset},{dhav.type.hex()},{dhav.subtype.hex()},{dhav.channel},{dhav.frame_number},{dhav.frame_subnumber},{dhav.frame_length},{dhav.timestamp}\n')


def timestamp_ok(timestamp, starttime, stoptime):
    if starttime and timestamp < starttime:
        return False
    if stoptime and timestamp > stoptime:
        return False
    return True

def get_frame(data, offset):
    # Get one DHAV frame
    while True:
        header_offset = data.find(b'DHAV', offset)
        if header_offset == -1:
            return None
        dhav = DHAVContext()
        data.seek(header_offset, 0)
        dhav.read_data(data)
        frametypes.add(dhav.type.hex())
        try:
            timestamp = date_to_timestamp(dhav.date)
            return (header_offset, dhav)
        except ValueError:
            logger.debug(f'Illegal date timestamp {dhav.date} at offset {header_offset} DHAVContext: {dhav}')
        # Keep searching
        offset = header_offset + 4


def get_cont_frames(data, offset, starttime, stoptime):
    # Get contiguous, non-fragmented DHAV frames
    # If starttime/stoptime is given, only frames in timeframe are returned
    frames = []
    while True:
        offset, frame = get_frame(data, offset) or (None, None)
        if frame:
            if not frame.type_ok():
                # We do not want these frametypes
                # Skip frame
                offset += 4
                continue
            if starttime or stoptime:
                # Check if timestamp is within requested timeframe
                timestamp = date_to_timestamp(frame.date)
                if not timestamp_ok(timestamp, starttime, stoptime):
                    # Skip frame
                    offset += 4
                    continue
            if frame.data[4:].find(b'DHAV') > -1:
                # Skip frames that have other DHAV frames within
                logger.debug(f'Frame at offset {offset} contains more than one DHAV header, skipping this header and will examine headers inside instead')
                offset += 4
                continue
            if frames:
                # This is not the first frame, we make sure it is contiguous
                if frames[-1][0] + frames[-1][1].frame_length == offset:
                    frames.append((offset, frame))
                else:
                    return frames
            else:
                frames.append((offset, frame))
            # Continue searching at end of last frame
            offset += frames[-1][1].frame_length
        else:
            return frames


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
        dhav_csv = open(os.path.join(outputfolder, 'dhav.csv'), 'w')
        heading = 'timestamp,offset,type,subtype,channel,frame_number,frame_subnumber,frame_length,extra_timestamp'
        dhav_csv.write(f'{heading}\n')
    
    # Setup logging
    levels = [logging.WARNING, logging.INFO, logging.DEBUG]
    level = levels[min(len(levels) - 1, args.get('verbosity'))]  # capped to number of levels
    logger.setLevel(level)
    handler = logging.StreamHandler()
    handler.setFormatter(Formatter())
    logger.addHandler(handler)

    
    with open(args.get('inputfile'), 'r+b') as f:
        if not args.get('dryrun'):
            videooutputfolder = os.path.join(outputfolder, 'video')
            if not os.path.exists(videooutputfolder):
                os.makedirs(videooutputfolder)

        # startoffset needs to be dividable by mmap.ALLOCATIONGRANULARITY, so we subtract if needed
        over = startoffset % mmap.ALLOCATIONGRANULARITY
        startoffset -= over
        offset = 0
        # memory-map the file
        mm = mmap.mmap(f.fileno(), length=0, offset=startoffset, access=mmap.ACCESS_READ)
        mapsize = mm.size()
        filesize = startoffset + mapsize
        frames = []
        
        # Start searching
        start_time = time.time()
        while True:
            frames = get_cont_frames(mm, offset, starttime, stoptime)
            if not frames:
                break
            found_time = time.time()
            found_location = frames[0][0]
            running_time = found_time - start_time
            eta = 'N/A'
            if running_time > 0:
                speed = found_location / running_time # bytes per second
                if speed > 0:
                    remaining = (mapsize - found_location) / speed # seconds remaining
                    eta = str(timedelta(seconds=remaining)).split('.', 2)[0]
            logger.info(f'Found {len(frames)} contiguous DHAV frames starting at offset {found_location}/{filesize} ({int(found_location/filesize*100)}%) ETA:{eta}')
            if not args.get('dryrun'):
                if args.get('h264'):
                    write_h264(videooutputfolder, frames)
                else:
                    write_dav(videooutputfolder, frames)
            if args.get('csv'):
                write_csv(dhav_csv, frames)

            # Keep searching at new offset
            offset = frames[-1][0] + frames[-1][1].frame_length

   
    if args.get('csv'):
        if dhav_csv:
            dhav_csv.close()
    print(f'Found the following frametypes: {frametypes}')


if __name__ == '__main__':
    main()
