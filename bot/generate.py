
def generate_horoscope(sign: str, tok, model) -> str:
    text = f"<s>{sign}\n"
    inpt = tok.encode(text, return_tensors="pt")

    out = model.generate(inpt.cpu(), max_length=200, repetition_penalty=5.0, do_sample=True,
                         top_k=5, top_p=0.95, temperature=1)

    return tok.decode(out[0]).replace('<s>', '').split("<")[0]

