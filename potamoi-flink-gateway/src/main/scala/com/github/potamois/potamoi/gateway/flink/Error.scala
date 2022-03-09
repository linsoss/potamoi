package com.github.potamois.potamoi.gateway.flink

/**
 * Error information.
 *
 * @param summary summary message
 * @param stack   exception stack
 * @author Al-assad
 */
case class Error(summary: String, stack: Throwable)
