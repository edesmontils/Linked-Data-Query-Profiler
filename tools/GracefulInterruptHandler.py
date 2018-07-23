#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-
"""
Control Ctrl-C signal ...
"""
#    From https://stackoverflow.com/questions/1112343/how-do-i-capture-sigint-in-python 

import signal
import time 

class GracefulInterruptHandler(object):
    def __init__(self, signals=(signal.SIGINT, signal.SIGTERM)):
        self.signals = signals
        self.original_handlers = {}

    def __enter__(self):
        self.interrupted = False
        self.released = False

        for sig in self.signals:
            self.original_handlers[sig] = signal.getsignal(sig)
            signal.signal(sig, self.handler)

        return self

    def handler(self, signum, frame):
        self.release()
        self.interrupted = True

    def __exit__(self, type, value, tb):
        self.release()

    def release(self):
        if self.released:
            return False

        for sig in self.signals:
            signal.signal(sig, self.original_handlers[sig])

        self.released = True
        return True

# class SIGINT_handler():
#     def __init__(self):
#         self.SIGINT = False

#     def signal_handler(self, signal, frame):
#         print('You pressed Ctrl+C!')
#         self.SIGINT = True


#==================================================
#==================================================
#==================================================
if __name__ == "__main__":
    with GracefulInterruptHandler() as h:
        for i in range(1000):
            print ("...")
            time.sleep(1)
            if h.interrupted:
                print ("interrupted!")
                time.sleep(2)
                # h.interrupted = False
                break
    print("The end!")

    # handler = SIGINT_handler()
    # signal.signal(signal.SIGINT, handler.signal_handler)

    # while True:
    #     # task
    #     if handler.SIGINT:
    #         break