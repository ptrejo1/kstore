package com.phoenix.kstore

class AbortTransactionException: Exception()

class OverflowException: Exception()

class ChecksumValidationException: Exception()

class TableOverflowException: Exception()

class InvalidRequestException(message: String): Exception(message)
