def convert_us_to_dec(odds):
    """
    Convert us ods to dec ods
    """
    new_odds = -100 / odds + 1 if odds <= -100 else odds / 100 + 1
    new_odds = round(new_odds, 3)
    return new_odds
