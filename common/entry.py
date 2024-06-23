import keyboard
import pygetwindow

class Stroker:
    def get_keystroke(allowed_keys=[], quit_key=None):
        terminal_title = pygetwindow.getActiveWindow().title
        while True:
            event = keyboard.read_event()
            if event.event_type == keyboard.KEY_DOWN:
                current_window = pygetwindow.getActiveWindow()
                if current_window and current_window.title == terminal_title:
                    key = event.name.upper()
                    if not quit_key:
                        break
                    elif key in [key.upper() for key in allowed_keys] + [quit_key]:
                        break
        if not quit_key:
            loop = False
        else:
            loop = key != quit_key.upper()
        return key, loop