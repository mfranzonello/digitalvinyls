import keyboard

class Stroker:
    def get_keystroke(allowed_keys=[], quit_key=None):
        while True:
            event = keyboard.read_event()
            if event.event_type == keyboard.KEY_DOWN:
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