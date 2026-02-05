import numpy as np
import pandas as pd

c = ['中国', '马来西亚', '墨西哥', '波兰', '南非', '中国台湾', '泰国', '越南', '印度',
     '印度尼西亚', '韩国', '俄罗斯联邦', '新加坡', '土耳其', '阿根廷', '智利', '哥伦比亚']

d = ['China', 'Malaysia', 'Mexico', 'Poland', 'South Africa', 'Taiwan', 'Thailand', 'Vietnam', 'India',
     'Indonesia', 'South Korea', 'Russia', 'Singapore', 'Turkey', 'Argentina', 'Chile', 'Colombia']

translate_dict = dict(zip(c, d))

def bfill_cny(s):
    '''Chinese New Year Seasonality Adjustment Function
    function to backfill march data to jan and feb
    s: pd series
    '''
    mask = (s.index.month == 1) | (s.index.month == 2)
    s.loc[mask] = np.nan
    mask = (s.index.month == 1) | (s.index.month == 2) | (s.index.month == 3)
    s.loc[mask] = s.loc[mask].bfill(limit=2)
    return s


def set_freq(s, China):
    if China:
        s = bfill_cny(s)
    s = s.dropna()
    if s.index.freq is None:
        infer_freq = pd.infer_freq(s.index)
        if infer_freq == 'M':
            s = s.asfreq('M')
        elif infer_freq == 'MS':
            s = s.asfreq('MS')
        else:
            print(f"-------{s.name} has unrecognized inferred frequency {infer_freq}------")
            s = s.asfreq('M')
    return s