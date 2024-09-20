from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnablePassthrough
from langchain_core.output_parsers import StrOutputParser
from langchain_groq import ChatGroq
from langchain_community.utilities import SQLDatabase

def get_sql_chain(db):
  template = """
    You are a data analyst at a company. You are interacting with a user who is asking you questions about the company's database.
    Based on the table schema below, write a SQL query that would answer the user's question. Take the conversation history into account.
    
    <SCHEMA>{schema}</SCHEMA>
    
    Conversation History: {chat_history}
    
    Write only the SQL query and nothing else. Do not wrap the SQL query in any other text, not even backticks.
    
    For example:
    Question: what is the last name of actor Jennifer?
    SQL Query: select last_name from actor where first_name = 'Jennifer';
    Question: give me the actor names who have worked in film - Chamber Italian
    SQL Query: select first_name as Actor_name from actor a inner join film_actor fa on a.actor_id = fa.actor_id
	            inner join film f on fa.film_id = f.film_id where f.title = 'Chamber Italian';
    
    Your turn:
    
    Question: {question}
    SQL Query:
    """
    
  prompt = ChatPromptTemplate.from_template(template)
  print(prompt)
  
  # llm = ChatOpenAI(model="gpt-4-0125-preview")
  llm = ChatGroq(model="llama-3.1-8b-instant", temperature=0)
  
  def get_schema(_):
    return db.get_table_info()
  
  return (
    RunnablePassthrough.assign(schema=get_schema)
    | prompt
    | llm
    | StrOutputParser()
  )

    
def get_response(user_query: str, db: SQLDatabase, chat_history: list):
  sql_chain = get_sql_chain(db)
  
  
  template = """
    You are a data analyst at a company. You are interacting with a user who is asking you questions about the company's database.
    Based on the table schema below, question, sql query, and sql response, write a natural language response.
    <SCHEMA>{schema}</SCHEMA>

    Conversation History: {chat_history}
    SQL Query: <SQL>{query}</SQL>
    User question: {question}
    SQL Response: {response}"""
  
  prompt = ChatPromptTemplate.from_template(template)
  
  # llm = ChatOpenAI(model="gpt-4-0125-preview")
  llm = ChatGroq(model="llama-3.1-8b-instant", temperature=0)
  
  chain = (
    RunnablePassthrough.assign(query=sql_chain).assign(
      schema=lambda _: db.get_table_info(),
      response=lambda vars: db.run(vars["query"]),
    )
    | prompt
    | llm
    | StrOutputParser()
  )
  
  return chain.invoke({
    "question": user_query,
    "chat_history": chat_history,
  })