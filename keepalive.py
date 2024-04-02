"""
Script to keep the computer awake during long data processes.
"""

import time
import random
import string
from pynput.keyboard import Controller

keyboard = Controller()

while True:
    random_letter = random.choice(string.ascii_lowercase)
    keyboard.type(random_letter)
    time.sleep(1)
