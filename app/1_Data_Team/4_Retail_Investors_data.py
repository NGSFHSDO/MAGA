import streamlit as st
from mlx_lm import load, generate

from langchain_core.prompts import PromptTemplate
from langchain_core.runnables import RunnableLambda
from langchain_core.output_parsers import StrOutputParser


st.set_page_config(page_title="Retail Investors LLM", page_icon="🤖")
st.title("Retail Investors LLM")


@st.cache_resource
def get_mlx_model():
    model_name = "mlx-community/Qwen3.5-35B-A3B-4bit"
    model, tokenizer = load(model_name)
    return model, tokenizer

def mlx_generate_from_text(text: str) -> str:
    model, tokenizer = get_mlx_model()

    messages = [
        {"role": "system", "content": "당신은 금융 용어를 쉽게 설명하는 어시스턴트입니다."},
        {"role": "user", "content": text},
    ]

    prompt = tokenizer.apply_chat_template(
        messages,
        add_generation_prompt=True,
    )

    output = generate(
        model,
        tokenizer,
        prompt=prompt,
        max_tokens=1024,
        verbose=False,
    )
    return output.strip()

prompt_tmpl = PromptTemplate.from_template("{question}")

chain = (
    prompt_tmpl
    | RunnableLambda(lambda prompt_value: mlx_generate_from_text(prompt_value.to_string()))
    | StrOutputParser()
)

user_question = st.text_input("질문을 입력하세요", placeholder="예: 괴리율이 뭐야?")

if st.button("질문하기", use_container_width=True):
    if not user_question.strip():
        st.warning("질문을 입력해 주세요.")
    else:
        with st.spinner("생성 중..."):
            answer = chain.invoke({"question": user_question})
        st.markdown("### 답변")
        st.write(answer)


st.set_page_config(
    page_title="Retail Investors Data",
    page_icon="👥",
)
st.title("Retail Investors Data")