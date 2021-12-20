package com.neutronstarer.npc

import java.util.Timer
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.schedule

typealias Cancel = () -> Unit

typealias Notify = (param: Any?) -> Unit

typealias Reply = (param: Any?, error: Any?) -> Unit

typealias Handle = (param: Any?, notify: Notify, reply: Reply) -> Cancel?

typealias Send = (message: Message) -> Unit

/**
 * NPC Near Procedure Call
 *
 * @constructor Create NPC
 *
 * @param send If send is null, you should extend NPC and override [send] function
 */
open class NPC constructor(send: Send?) {

    /**
     * On Register handle for method
     *
     * @param method Method name
     * @param handle Handle
     */
    open fun on(method: String, handle: Handle) {
        _lock.lock()
        _handles[method] = handle
        _lock.unlock()
    }

    /**
     * Emit Emit a message without reply
     *
     * @param method Method name
     * @param param Method param
     */
    @JvmOverloads
    open fun emit(method: String, param: Any? = null) {
        _send(Message(typ = Typ.Emit, param = param))
    }

    /**
     * Deliver Deliver a message with reply
     *
     * @param method Method name
     * @param param Method param
     * @param timeout Timeout in millisecond
     * @param onNotify Notify callback
     * @param onReply Reply callback
     * @return [Cancel] A function to cancel delivering
     */
    @JvmOverloads
    open fun deliver(
        method: String,
        param: Any? = null,
        timeout: Long = 0,
        onNotify: Notify? = null,
        onReply: Reply? = null
    ): Cancel {
        val id = _id
        _lock.lock()
        _id++
        _lock.unlock()
        val completedLock = ReentrantLock()
        var completed = false
        var timer: Timer? = null
        val reply = Reply@{ p: Any?, error: Any? ->
            completedLock.lock()
            if (completed) {
                completedLock.unlock()
                return@Reply false
            }
            completed = true
            completedLock.unlock()
            if (onReply != null) {
                onReply(p, error)
            }
            timer?.cancel()
            return@Reply true
        }
        _lock.lock()
        if (onNotify != null) {
            _notifies[id] = Notify@{ p: Any? ->
                completedLock.lock()
                if (completed) {
                    completedLock.unlock()
                    return@Notify
                }
                completedLock.unlock()
                onNotify(p)
            }
        }
        _replies[id] = reply
        _lock.unlock()
        if (timeout > 0) {
            timer = Timer()
            timer.schedule(timeout) {
                if (reply(null, "timedout")) {
                    _send(Message(typ = Typ.Cancel, id = id))
                }
            }
        }
        _send(Message(typ = Typ.Deliver, id = id, method = method, param = param))
        return {
            if (reply(null, "cancelled")) {
                _send(Message(typ = Typ.Cancel, id = id))
            }
        }
    }

    /**
     * Send Send message, usually override it if need
     *
     * @param message Message
     */
    open fun send(message: Message) {

    }

    /**
     * Receive Receive message
     *
     * @param message Message
     */
    open fun receive(message: Message) {
        when (message.typ) {
            Typ.Emit -> {
                _lock.lock()
                val handle = _handles[message.method]
                _lock.unlock()
                if (handle != null) {
                    handle(message.param, { }, { _, _ -> })
                }
            }

            Typ.Deliver -> {
                val method = message.method?:return
                val id = message.id ?: return
                _lock.lock()
                val handle = _handles[method]
                _lock.unlock()
                if (handle == null){
                    return
                }
                val completedLock = ReentrantLock()
                var completed = false
                val cancel = handle(message.param, Notify@{ param ->
                    completedLock.lock()
                    if (completed){
                        completedLock.unlock()
                        return@Notify
                    }
                    completedLock.unlock()
                    _send(Message(typ = Typ.Notify, id = id, param = param))
                }, Reply@{ param, error ->
                    completedLock.lock()
                    if (completed){
                        completedLock.unlock()
                        return@Reply
                    }
                    completed = true
                    completedLock.unlock()
                    _lock.lock()
                    _cancels.remove(id)
                    _lock.unlock()
                    _send(Message(typ = Typ.Ack, id = id, param = param, error = error))
                })
                if (cancel != null){
                    _lock.lock()
                    _cancels[id] = Cancel@{
                        completedLock.lock()
                        if (completed){
                            completedLock.unlock()
                            return@Cancel
                        }
                        completed = true
                        completedLock.unlock()
                        _lock.lock()
                        _cancels.remove(id)
                        _lock.unlock()
                        cancel()
                    }
                    _lock.unlock()
                }
            }

            Typ.Ack -> {
                _lock.lock()
                val reply = _replies[message.id]
                _lock.unlock()
                if (reply != null){
                    reply(message.param, message.error)
                }
            }

            Typ.Notify -> {
                _lock.lock()
                val notify = _notifies[message.id]
                _lock.unlock()
                if(notify != null){
                    notify(message.param)
                }
            }

            Typ.Cancel -> {
                _lock.lock()
                val cancel = _cancels[message.id]
                _lock.unlock()
                if (cancel != null) {
                    cancel()
                }
            }
        }
    }

    private var _id = 0
    private var _notifies = mutableMapOf<Int, Notify>()
    private val _cancels = mutableMapOf<Int, Cancel>()
    private val _replies = mutableMapOf<Int, (param: Any?, error: Any?) -> Boolean>()
    private val _handles = mutableMapOf<String, Handle>()
    private val _lock = ReentrantLock()
    private val _send: Send = if (send == null) {
        { message ->
            send(message)
        }
    } else {
        send
    }
}

/**
 * Typ Message Type
 *
 * @property rawValue rawValue
 * @constructor Create empty Typ
 */
enum class Typ(val rawValue: Int) {
    /**
     * Emit
     *
     * @constructor Create empty Emit
     */
    Emit(0),

    /**
     * Deliver
     *
     * @constructor Create empty Deliver
     */
    Deliver(1),

    /**
     * Notify
     *
     * @constructor Create empty Notify
     */
    Notify(2),

    /**
     * Ack
     *
     * @constructor Create empty Ack
     */
    Ack(3),

    /**
     * Cancel
     *
     * @constructor Create empty Cancel
     */
    Cancel(4)
}

/**
 * Message
 *
 * @property typ Message type
 * @property id Message id
 * @property method Message method
 * @property param Message param
 * @property error Message error
 * @constructor Create empty Message
 */
class Message constructor(
    val typ: Typ,
    val id: Int? = null,
    val method: String? = null,
    val param: Any? = null,
    val error: Any? = null
){
    override fun toString(): String {
        val v = mutableMapOf<String,Any>()
        v["typ"] = typ.rawValue
        if (id != null){
            v["id"] = id
        }
        if (method != null){
            v["method"] = method
        }
        if (param != null){
            v["param"] = param
        }
        if (error != null){
            v["error"] = error
        }
        return v.toString()
    }
}