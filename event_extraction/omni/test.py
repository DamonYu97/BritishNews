from event_extraction.OmniEvent.extractor import Extractor

if __name__ == '__main__':
    extractor = Extractor()

    # Even Extraction (EE) Task
    text = ["2022年北京市举办了冬奥会, 并且中国拿了20枚金牌", "U.S. and British troops were moving on the strategic southern port city of Basra Saturday after a massive aerial assault pounded Baghdad at dawn"]
    print(extractor.extract_events(text))