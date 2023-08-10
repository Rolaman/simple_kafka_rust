# Task

It is necessary to design and implement a simple in-memory message broker. The broker divides the message streams according to so-called topics.
Each message is associated with an optional key. The format of the message and the key is determined by the candidate (for example, a byte array or JSON).
The interaction protocol is determined by the candidate (for example, TCP or HTTP+sse).

The following requests must be available:

- a. Creation of a topic
- b. Subscription to messages in a topic
- c. Unsubscription from messages in a topic
- d. Adding a message to a topic

Will be a plus:
* a retention parameter for topics, indicating the time after which the message becomes unavailable
* a compaction parameter (https://kafka.apache.org/documentation/#compaction) for topics: only the last message with the same key remains
* a commit mechanism: a message is considered delivered only if the consumer has explicitly notified about it

  Language: Rust
  Use of libraries/frameworks at the candidate's discretion.

_______

Необходимо спроектировать и реализовать простой in-memory брокер сообщений. Брокер разделяет потоки сообщений по т.н. топикам. С каждым сообщением связан опциональный ключ. Формат сообщения и ключа определяется кандидатом (например байтовый массив или json). Протокол взаимодействия определяется кандидатом (например tcp или http+sse).
Должны быть доступны следующие запросы:

- a. Создание топика
- b. Подписка на сообщения в топике
- c. Отписка от сообщений в топике
- d. Добавление сообщения в топик

Будет плюсом:
* параметр retention для топиков, который указывает на время, после которого сообщение становится недоступным
* параметр compaction (https://kafka.apache.org/documentation/#compaction) для топиков: из сообщений с одинаковым ключом остаётся только последнее
* механизм commit: сообщение считается доставленным только если потребитель явно об этом сообщил
  Язык: Rust
  Использование библиотек/фреймворков на усмотрение кандидата.
