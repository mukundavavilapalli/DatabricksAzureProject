# Databricks notebook source
pip install jinja2

# COMMAND ----------

parameters = [
    {
        "table":"spotify_cata.silver.factstream",
        "alias":"fact",
        "columns":"fact.stream_id,fact.listen_duration"
    },
    {
        "table":"spotify_cata.silver.dimuser",
        "alias":"user",
        "columns":"user.user_id,user.user_name",
        "condition":"fact.user_id = user.user_id"
    },
    {
        "table":"spotify_cata.silver.dimtrack",
        "alias":"track",
        "columns":"track.track_id,track.track_name",
        "condition":"fact.track_id = track.track_id"
    }
]

# COMMAND ----------

from jinja2 import Template

query_text = """
        SELECT
          {% for params in parameters %}
            {{ params.columns}}
            {% if not loop.last %}
               ,
            {% endif %}
          {% endfor %}
        FROM
            {% for params in parameters %}
                {{params.table}} AS {{params.alias}}
                {% if not loop.first %}
                    ON
                    {{ params.condition }}
                {% endif %}
                {% if not loop.last %}
                    LEFT JOIN
                {% endif %}
            {% endfor %}
"""

# COMMAND ----------

query = Template(query_text).render(parameters = parameters)
print(query)

# COMMAND ----------

spark.sql(query).display()