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
class NPC {

    private var send: Send? = null
    private var id = -2147483648
    private val notifies: MutableMap<Int, Notify> by lazy {
        mutableMapOf()
    }
    private val cancels: MutableMap<Int, Cancel> by lazy {
        mutableMapOf()
    }
    private val replies: MutableMap<Int, (param: Any?, error: Any?) -> Boolean> by lazy {
        mutableMapOf()
    }
    private val handles: MutableMap<String, Handle> by lazy {
        mutableMapOf()
    }
    private val lock: ReentrantLock by lazy {
        ReentrantLock()
    }


    /**
     * Connect
     * @param send Send
     * 保证最后一个connect是有效的
     */
    fun connect(send: Send){
        disconnect(null)
        lock.lock()
        try {
            this.send = send
        } finally {
            lock.unlock()
        }
    }

    /**
     * Disconnect
     * @param reason reason, default "disconnected"
     */
    fun disconnect(reason: Any?){
        val error = reason ?: "disconnected"
        val replies = this.replies.values
        val cancels = this.cancels.values
        replies.forEach {
            it(null, error)
        }
        cancels.forEach {
            it()
        }
        lock.lock()
        this.send = null
        lock.unlock()
    }

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
        return handles[method]
    }
    operator fun set(method: String, handle: Handle?) {
        lock.lock()
        if (handle == null){
            handles.remove(method)
        }else{
            handles[method] = handle
        }
        lock.unlock()
    }
    /**
     * Emit Emit a message without reply
     *
     * @param method Method name
     * @param param Method param
     */
    fun emit(method: String, param: Any? = null) {
        lock.lock()
        try {
            send?.invoke(Message(typ = Typ.Emit, id = nextId(), method = method, param = param))
        } catch (e: InterruptedException) {
            Thread.currentThread().interrupt()
        } finally {
            lock.unlock()
        }
    }

    /**
     * Deliver Deliver a message with reply
     *
     * @param method Method name
     * @param param Method param
     * @param timeout Timeout in millisecond
     * @param onReply Reply callback
     * @param onNotify Notify callback
     * @return [Cancel] A function to cancel delivering
     */
    fun deliver(
        method: String,
        param: Any? = null,
        timeout: Long = 0,
        onReply: Reply? = null,
        onNotify: Notify? = null
    ): Cancel {
        lock.lock()
        val id = nextId()
        var completed = false
        var timer: Timer? = null
        var _onReply = onReply
        val reply = Reply@{ p: Any?, error: Any? ->
            if (completed) {
                return@Reply false
            }
            completed = true
            timer?.cancel()
            _onReply?.invoke(p, error)
            timer = null
            _onReply = null
            replies.remove(id)
            notifies.remove(id)
            return@Reply true
        }
        replies[id] = reply
        onNotify?.let {
            notifies[id] = Notify@{ p: Any? ->
                if (completed) {
                    return@Notify
                }
                it.invoke(p)
            }
        }
        if (timeout > 0) {
            timer = Timer()
            timer!!.schedule(timeout) {
                if (reply(null, "timedout")) {
                    send?.invoke(Message(typ = Typ.Cancel, id = id))
                }
            }
        }
        send?.invoke(Message(typ = Typ.Deliver, id = id, method = method, param = param))
        lock.unlock()
        return {
            if (reply(null, "cancelled")) {
                send?.invoke(Message(typ = Typ.Cancel, id = id))
            }
        }
    }
    /**
     * Clean up all deliveries with special reason.
     *
     * @param reason Error
     */

    /**
     * Receive Receive message
     *
     * @param message Message
     */
    fun receive(message: Message) {
        when (message.typ) {
            Typ.Emit -> {
                val handle = handles[message.method]
                if (handle == null) {
                    println("[NPC] unhandled message: ${message}")
                    return
                }
                handle.invoke(message.param, { _, _ -> }, { })
            }
            Typ.Deliver -> {
                val id = message.id
                val handle = handles[message.method]
                if (handle == null){
                    println("[NPC] unhandled message: ${message}")
                    send?.invoke(Message(typ = Typ.Ack, id = message.id, param = null, error = "unimplemented"))
                    return
                }
                var completed = false
                val cancel = handle(message.param, Reply@{ param, error ->
                    if (completed){
                        return@Reply
                    }
                    completed = true
                    cancels.remove(id)
                    send?.invoke(Message(typ = Typ.Ack, id = id, param = param, error = error))
                }, Notify@{ param ->
                    if (completed){
                        return@Notify
                    }
                    send?.invoke(Message(typ = Typ.Notify, id = id, param = param))
                })
                if (cancel != null){
                    lock.lock()
                    cancels[id] = Cancel@{
                        if (completed){
                            return@Cancel
                        }
                        completed = true
                        cancels.remove(id)
                        cancel()
                    }
                    lock.unlock()
                }
            }
            Typ.Ack -> {
                val reply = replies[message.id]
                reply?.invoke(message.param, message.error)
            }
            Typ.Notify -> {
                val notify = notifies[message.id]
                notify?.invoke(message.param)
            }
            Typ.Cancel -> {
                val cancel = cancels[message.id]
                cancel?.invoke()
            }
        }
    }
    private fun nextId(): Int {
        if (id < 2147483647){
            id++
        }else{
            id = -2147483647
        }
        return id
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
data class Message(
    val typ: Typ,
    val id: Int,
    val method: String? = null,
    val param: Any? = null,
    val error: Any? = null
){
    override fun toString(): String {
        val v = mutableMapOf<String,Any>().apply {
            this["typ"] = typ.rawValue
            this["id"] = id
            method?.let {
                this["method"] = method
            }
            param?.let {
                this["param"] = param
            }
            error?.let {
                this["error"] = error
            }
        }
        return v.toString()
    }
}