package com.neutronstarer.npc

import java.util.Timer
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.schedule

typealias Cancel = () -> Unit

typealias Notify = (param: Any?) -> Unit

typealias Reply = (param: Any?, error: Any?) -> Unit

typealias Handle = (param: Any?, reply: Reply, notify: Notify) -> Cancel?

typealias Send = (message: Message) -> Unit

/**
 * NPC Near Procedure Call
 *
 * @constructor Create NPC
 */
public final class NPC() {

    lateinit var send: Send
    /**
     * On Register handle for method
     *
     * @param method Method name
     * @param handle Handle
     */
    fun on(method: String, handle: Handle?) {
        this[method] = handle
    }
    operator fun get(method: String): Handle? {
        _lock.lock()
        val handle = _handles[method]
        _lock.unlock()
        return handle
    }
    operator fun set(method: String, handle: Handle?) {
         _lock.lock()
        if (handle == null){
            _handles.remove(method)
        }else{
            _handles[method] = handle
        }
        _lock.unlock()
    }
    /**
     * Emit Emit a message without reply
     *
     * @param method Method name
     * @param param Method param
     */
    fun emit(method: String, param: Any? = null) {
        _lock.lock()
        val id = nextId()
        _lock.unlock()
        val m = Message(typ = Typ.Emit, id = id, method = method, param = param)
        send(m)
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
    fun deliver(
        method: String,
        param: Any? = null,
        timeout: Long = 0,
        onReply: Reply? = null,
        onNotify: Notify? = null
    ): Cancel {
        _lock.lock()
        val id = nextId()
        val completedLock = ReentrantLock()
        var completed = false
        var timer: Timer? = null
        var _onReply = onReply
        val reply = Reply@{ p: Any?, error: Any? ->
            completedLock.lock()
            if (completed) {
                completedLock.unlock()
                return@Reply false
            }
            completed = true
            completedLock.unlock()
            timer?.cancel()
            _onReply?.invoke(p, error)
            timer = null
            _onReply = null
            _lock.lock()
            _replies.remove(id)
            _notifies.remove(id)
            _lock.unlock()
            return@Reply true
        }
        _replies[id] = reply
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
        _lock.unlock()
        if (timeout > 0) {
            timer = Timer()
            timer!!.schedule(timeout) {
                if (reply(null, "timedout")) {
                    val m = Message(typ = Typ.Cancel, id = id)
                    send(m)
                }
            }
        }
        val m = Message(typ = Typ.Deliver, id = id, method = method, param = param)
        send(m)
        return {
            if (reply(null, "cancelled")) {
                val mm = Message(typ = Typ.Cancel, id = id)
                send(mm)
            }
        }
    }
    /**
     * Clean up all deliveries with special reason.
     *
     * @param reason Error
     */
    fun cleanUp(reason: Any?){
        _lock.lock()
        val values = _replies.values
        _lock.unlock()
        values.forEach {
            it(null, reason)
        }
    }

    /**
     * Receive Receive message
     *
     * @param message Message
     */
    fun receive(message: Message) {
        when (message.typ) {
            Typ.Emit -> {
                val method = message.method
                if (method == null){
                    println("[NPC] unhandled message: ${message}")
                    return
                }
                _lock.lock()
                val handle = _handles[method]
                _lock.unlock()
                if (handle == null){
                    println("[NPC] unhandled message: ${message}")
                    return
                }
                handle.invoke(message.param, { _, _ -> }, { })
            }

            Typ.Deliver -> {
                val method = message.method
                if (method == null){
                    println("[NPC] unhandled message: ${message}")
                    return
                }
                val id = message.id
                _lock.lock()
                val handle = _handles[message.method]
                _lock.unlock()
                if (handle == null){
                    println("[NPC] unhandled message: ${message}")
                    val m = Message(typ = Typ.Ack, id = message.id, param = null, error = "unimplemented")
                    send(m)
                    return
                }
                val completedLock = ReentrantLock()
                var completed = false
                val cancel = handle(message.param, Reply@{ param, error ->
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
                    val m = Message(typ = Typ.Ack, id = id, param = param, error = error)
                    send(m)
                }, Notify@{ param ->
                    completedLock.lock()
                    if (completed){
                        completedLock.unlock()
                        return@Notify
                    }
                    completedLock.unlock()
                    val m = Message(typ = Typ.Notify, id = id, param = param)
                    send(m)
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
                reply?.invoke(message.param, message.error)
            }

            Typ.Notify -> {
                _lock.lock()
                val notify = _notifies[message.id]
                _lock.unlock()
                notify?.invoke(message.param)
            }

            Typ.Cancel -> {
                _lock.lock()
                val cancel = _cancels[message.id]
                _lock.unlock()
                cancel?.invoke()
            }
        }
    }
    private fun nextId(): Int{
        if (_id < 2147483647){
            _id++
        }else{
            _id = -2147483647
        }
        return _id
    }
    private var _id = -2147483648
    private var _notifies = mutableMapOf<Int, Notify>()
    private val _cancels = mutableMapOf<Int, Cancel>()
    private val _replies = mutableMapOf<Int, (param: Any?, error: Any?) -> Boolean>()
    private val _handles = mutableMapOf<String, Handle>()
    private val _lock = ReentrantLock()
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
    Cancel(4);

    companion object {
        fun fromRawValue(rawValue: Int) = Typ.values().first { it.rawValue == rawValue }
    }
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
    val id: Int,
    val method: String? = null,
    val param: Any? = null,
    val error: Any? = null
){
    override fun toString(): String {
        val v = mutableMapOf<String,Any>()
        v["typ"] = typ.rawValue
        v["id"] = id
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