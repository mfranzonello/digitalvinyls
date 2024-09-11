''' Language processing '''

import re

class Colors:
    RED = '\033[91m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    MAGENTA = '\033[95m'
    CYAN = '\033[96m'
    WHITE = '\033[97m'
    GREY = '\033[90m'
    END = '\033[0m'  # Reset to default color
    
    RED_RGB = (255, 0, 0)
    GREEN_RGB = (0, 255, 0)
    YELLOW_RGB = (255, 255, 0)

    def get_color(r, g, b):
        return f'\033[38;2;{r};{g};{b}m'

    def scale_color(pct, start, end, middle=None):
        if middle:
            if pct < 0.5:
                end = middle
                pct = 2 * pct
            else:
                start = middle
                pct = 2 * pct - 1
        r, g, b = (round(s + (e-s) * pct) for s, e in zip(start, end))
        color = Colors.get_color(r, g, b)
        return color

class Texter:
    def __init__(self):
        pass

    def clean_text(self, text:str) -> str:
        cleaned_text = self.emoji_pattern.sub(r'', text).replace(self.zerowidthjoiner,'').strip()
        return cleaned_text

    def slashable(self, char):
        slash_chars = ['[', '(', ']', ')', '.', '\\', '-']
        slash = '\\' if char in slash_chars else ''
        return slash

    def remove_parentheticals(self, text, removes):
        for remove in removes:
            text, captured = self.remove_parenthetical(text, **remove)
            # if captured then break
            
        return text, captured

    # # def remove_parentheticals(self, text, words):
    # #     spaces = '(?:\s+\S*)?\s*'
    # #     pattern = r'|'.join(rf'\{p[0]}{spaces}{word}{spaces}\{p[1]}' for p in ['()', '[]'] for word in words)

    # #     pattern = r'\((?:\s+\S*)?\s*deluxe(?:\s+\S*)?\s*\)'

    def remove_parenthetical(self, text, words, position, parentheses=[['(', ')'], ['[', ']']],
                             middle=None, case_sensitive=False):
        captured = None
        if text:

            punctuation = '!,.;()/\\-[]'
            punctuation_s = '^' + punctuation
            punctuation_e = punctuation + '$'

            if parentheses == 'all':
                parentheses = [[start, end] for start in punctuation_s for end in punctuation_e]
            elif parentheses == 'all_start':
                [[start, ''] for start in punctuation_s]
            elif parentheses == 'all_end':
                parentheses = [['', end] for end in punctuation_e]

            capture_s = '(.*?)' if position == 'end' else ''
            capture_e = '(.*?)' if position == 'start' else ''
            capture_m = f'.*?{middle}.*?' if middle else ''

            pattern = '|'.join(f'({self.slashable(s)}{s}{capture_s}'
                               f'{w}{capture_m}'
                               f'{capture_e}{self.slashable(e)}{e})' for w in words for s, e in parentheses)
        
            flags = re.IGNORECASE if not case_sensitive else 0

            searched = re.search(pattern, text, flags=flags)
            if searched:
                # find the shortest captured text
                captured = sorted((s for s in searched.groups()[1::2] if (s is not None)), key=len)[0].strip()
                replaceable = sorted((s for s in searched.groups()[0::2] if (s is not None)), key=len)[0].strip()
                
                text = text.replace(replaceable, '').strip()
            
        return text, captured

    def drop_dash(self, text):
        # remove description after dash
        if ' - ' in text:
            p = text.find(' - ')
            if p > 0 and not ((text[:p].count('(') > text[:p].count(')')) and (text[p:].find(')') > text[p:].find('('))):
                # don't drop if - is in between parenthesis
                text = text[:text.find(' - ')].strip()
        if text[-2:] == ' -':
            text = text[:-2]

        return text
    
    def remove_parentheticals_and_drop_dash(self, text, removes):
        text, captured = self.remove_parentheticals(text, removes)
        text = self.drop_dash(text)
        return text, captured