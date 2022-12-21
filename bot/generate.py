from transformers import GPT2LMHeadModel, GPT2Tokenizer
from kafkaConsumer import kafkaConsumer
from kafkaProducer import kafkaProducer

import logging

# loading tokens and pre-trained model
tok = GPT2Tokenizer.from_pretrained("models/essays")
model = GPT2LMHeadModel.from_pretrained("models/essays")
# switching to cpu
model.cpu()

logging.basicConfig(level=logging.INFO)


def generate_horoscope(sign: str) -> str:
    text = f"<s>{sign}\n"
    logging.info(f'Generating horoscope for {sign}')
    inpt = tok.encode(text, return_tensors="pt")

    out = model.generate(inpt.cpu(), max_length=200, repetition_penalty=5.0, do_sample=True,
                         top_k=5, top_p=0.95, temperature=1)

    return tok.decode(out[0]).replace('<s>', '').split("<")[0]


def main(con: kafkaConsumer, prod: kafkaProducer):
    while True:
        message = con.read_from_topic()
        if message == 0:
            continue
        else:
            print('Going to decode message:: ', message)
            message_string = message.decode('utf-8')
            print(f'Decoded message: {message_string}')
            # wait for 2 seconds before next message
            horoscope = generate_horoscope(message_string)
            print(f'Pushing horoscope to kafka: {horoscope}')
            prod.push(horoscope)


if __name__ == '__main__':
    print('Started generate.py')
    consumer = kafkaConsumer('topic1', 0, 'group1', 'localhost:9092')
    producer = kafkaProducer('localhost:9092', 'topic2')
    main(consumer, producer)
