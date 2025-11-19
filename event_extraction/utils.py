import re

from gcloud_storage import GoogleCloudStorage

PROJECT_ID = "frances-365422"
BUCKET_NAME = "eidf_gpu"

cloud_storage_service = None

def get_google_cloud_storage():
    global cloud_storage_service
    if cloud_storage_service is not None:
        return cloud_storage_service
    cloud_storage_service = GoogleCloudStorage(project_id=PROJECT_ID, bucket_name=BUCKET_NAME)
    return cloud_storage_service

# chunk text
def ends_with(options, text):
    for option in options:
        if text.endswith(option):
            return True
    return False

def chunk(text, max_sequence_length=512):
    sentences = re.split(r'(?<=\.)\s+', text)
    # remove the last empty sentence if exists
    if len(sentences[-1]) == 0:
        sentences.pop(-1)
    offsets = []
    previous_end = 0
    for index, sentence in enumerate(sentences):
        if len(sentence) == 0:
            print(previous_end)
            raise Exception('Empty sentence')
        sent_start = text.find(sentence, previous_end)
        sent_end = sent_start + len(sentence)
        if sent_start < 0:
            raise Exception("Cannot find start index of the sentence")
        previous_end = sent_end
        offsets.append({'start': sent_start, 'end': sent_end})
    #print(len(offsets))
    # if sentence ends with specified chars, then the next sentence will be added
    ignores = ["Mr.", "Mrs.", "Ms.", "Miss", "Dr.", "Prof.", "Rev.", "Gen.", "Col.", "Maj.", "Lt.", "Sgt.", "Capt.",
    "Gov.", "Sen.", "Rep.", "Pres.", "Amb.", "Hon.", "Atty.", "Fr.", "Br.", "Sr.", "Fig."]
    s_size = len(sentences)
    s_index = 0
    while (s_index < s_size):
        if ends_with(ignores, sentences[s_index]):
          if s_index + 1 < s_size:
            sentences[s_index] += " " + sentences[s_index + 1]
            sentences.pop(s_index + 1)
            offsets[s_index]['end'] = offsets[s_index + 1]['end']
            offsets.pop(s_index + 1)
            #print("-----specified chars")
            #print(sentences[s_index])
            s_size -= 1
            continue
        s_index += 1

    # if the next sentence starts with lowercase char, then it will be added
    s_size = len(sentences)
    s_index = 1
    while (s_index < s_size):
        if sentences[s_index][0].islower():
            sentences[s_index - 1] += " " + sentences[s_index]
            sentences.pop(s_index)
            offsets[s_index - 1]['end'] = offsets[s_index]['end']
            offsets.pop(s_index)
            #print("-----lowercase chars")
            #print(sentences[s_index-1])
            s_size -= 1
            continue
        s_index += 1
    #print(len(offsets))

    # if sentence has less than max_sequence_length words, then it will be added to previous sentence
    s_size = len(sentences)
    s_index = 0
    while (s_index < s_size):
        len_current_sequence = len(sentences[s_index].split())
        if len_current_sequence < max_sequence_length:
          if s_index + 1 < s_size and len(sentences[s_index + 1].split()) + len_current_sequence < max_sequence_length:
            sentences[s_index] += " " + sentences[s_index + 1]
            sentences.pop(s_index + 1)
            offsets[s_index]['end'] = offsets[s_index + 1]['end']
            offsets.pop(s_index + 1)
            s_size -= 1
            continue
        s_index += 1
    sentences = [text[offset['start']:offset['end']] for offset in offsets]
    return sentences, offsets