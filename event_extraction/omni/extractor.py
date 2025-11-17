import time

import torch
from OmniEvent.infer import get_pretrained
from OmniEvent.infer_module.seq2seq import do_event_detection, prepare_for_eae_from_pred, do_event_argument_extraction, \
    get_eae_result


class Extractor:
    def __init__(self, device="auto"):
        if device == 'auto':
            self.device = torch.device("cpu")
            if torch.cuda.is_available():
                self.device = torch.device("cuda")
        else:
            self.device = torch.device(device)
        print(f"Using device: {self.device}")
        self.ed_model, self.ed_tokenizer = get_pretrained("s2s-mt5-ed", self.device)
        self.eae_model, self.eae_tokenizer = get_pretrained("s2s-mt5-eae", self.device)

    def extract_events(self, texts, schema="ace"):
        schemas = len(texts) * [f"<{schema}>"]
        print(f"Extracting events for {len(texts)} texts...")
        print(f"Running event detection.....")
        start_time = time.time()
        events = do_event_detection(self.ed_model, self.ed_tokenizer, texts, schemas, self.device)
        print("Running event argument extraction...")
        instances = prepare_for_eae_from_pred(texts, events, schemas)
        arguments = do_event_argument_extraction(self.eae_model, self.eae_tokenizer, instances, self.device)
        print(f"Getting event extraction results...")
        results = get_eae_result(instances, arguments)
        print(f"Event extraction is completed in {time.time() - start_time:.2f} seconds.")
        return results

