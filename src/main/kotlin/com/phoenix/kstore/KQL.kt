package com.phoenix.kstore

import kotlinx.coroutines.*
import org.petitparser.parser.primitive.CharacterParser
import org.petitparser.parser.primitive.CharacterParser.word
import org.petitparser.parser.primitive.StringParser

interface KQLResponse

class OperationResponse(val batchResponse: BatchResponse?): KQLResponse

class MessageResponse(val message: String): KQLResponse

class KQL(private val node: Node) {

    private val scope = CoroutineScope(Dispatchers.Default)

    private val key = CharacterParser.of('/')
        .seq(word().plus())
        .seq(CharacterParser.of('/'))
        .seq(word().plus())
        .flatten()

    private val get = StringParser.ofIgnoringCase("GET")
        .seq(key.trim())
        .map<List<String>, Request> { values ->
            GetRequest(values[1].toByteArray())
        }

    private val put = StringParser.ofIgnoringCase("PUT")
        .seq(key.trim())
        .seq(word().plus().flatten().trim())
        .map<List<String>, Request> { values ->
            PutRequest(values[1].toByteArray(), values[2].toByteArray())
        }

    private val delete = StringParser.ofIgnoringCase("DELETE")
        .seq(key.trim())
        .map<List<String>, Request> { values ->
            DeleteRequest(values[1].toByteArray())
        }

    private val operation = get.or(put, delete)

    private val transaction = StringParser.ofIgnoringCase("BEGIN")
        .seq(operation.plus().trim())
        .seq(StringParser.ofIgnoringCase("END").trim())
        .map<List<Any>, BatchRequest> { values ->
            @Suppress("UNCHECKED_CAST")
            BatchRequest(values[1] as List<Request>)
        }

    private val info = StringParser.ofIgnoringCase("INFO").trim()

    private val statementParser = operation
        .or(transaction, info)
        .seq(CharacterParser.of(';'))
        .map<List<Any>, KQLResponse> { values ->
            val operation = values[0]
            if (operation.toString().lowercase() == "info") {
                return@map MessageResponse(node.toMap().toString())
            }

            if (operation is BatchRequest) {
                val response = scope.async { node.router.request(operation) }
                return@map OperationResponse(runBlocking { response.await() })
            }

            @Suppress("UNCHECKED_CAST")
            val batchReq = BatchRequest(listOf(operation) as List<Request>)
            val response = scope.async { node.router.request(batchReq) }
            OperationResponse(runBlocking { response.await() })
        }

    fun parse(statement: String): KQLResponse {
        val result = statementParser.parse(statement)
        if (result.isFailure) return MessageResponse(result.message)

        return result.get()
    }
}