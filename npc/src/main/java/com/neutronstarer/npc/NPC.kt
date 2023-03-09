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
public final class NPC {

    /**
     * Connect
     * @param send Send
     */
    fun connect(send: Send){
        disconnect(null)
        lock.lock()
        this.send = send
        lock.unlock()
    }

    /**
     * Disconnect
     * @param reason reason, default "disconnected"
     */
    fun disconnect(reason: Any?){
        val error = if (reason == null) "disconnected" else reason
        lock.lock()
        val replies = this.replies.values
        val cancels = this.cancels.values
        replies.forEach {
            it(null, error)
        }
        cancels.forEach {
            it()
        }
        send = null
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
        lock.lock()
        val handle = handles[method]
        lock.unlock()
        return handle
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
        val id = nextId()
        val m = Message(typ = Typ.Emit, id = id, method = method, param = param)
        send?.invoke(m)
        lock.unlock()
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
        if (onNotify != null) {
            notifies[id] = Notify@{ p: Any? ->
                if (completed) {
                    return@Notify
                }
                onNotify(p)
            }
        }
        if (timeout > 0) {
            timer = Timer()
            timer!!.schedule(timeout) {
                lock.lock()
                if (reply(null, "timedout")) {
                    val m = Message(typ = Typ.Cancel, id = id)
                    send?.invoke(m)
                }
                lock.unlock()
            }
        }
        val m = Message(typ = Typ.Deliver, id = id, method = method, param = param)
        send?.invoke(m)
        lock.unlock()
        return {
            lock.lock()
            if (reply(null, "cancelled")) {
                val mm = Message(typ = Typ.Cancel, id = id)
                send?.invoke(mm)
            }
            lock.unlock()
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
        lock.lock()
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
                    val m = Message(typ = Typ.Ack, id = message.id, param = null, error = "unimplemented")
                    send?.invoke(m)
                    return
                }
                var completed = false
                val cancel = handle(message.param, Reply@{ param, error ->
                    lock.lock()
                    if (completed){
                        lock.unlock()
                        return@Reply
                    }
                    completed = true
                    cancels.remove(id)
                    val m = Message(typ = Typ.Ack, id = id, param = param, error = error)
                    send?.invoke(m)
                    lock.unlock()
                }, Notify@{ param ->
                    lock.lock()
                    if (completed){
                        lock.unlock()
                        return@Notify
                    }
                    val m = Message(typ = Typ.Notify, id = id, param = param)
                    send?.invoke(m)
                    lock.unlock()
                })
                if (cancel != null){
                    cancels[id] = Cancel@{
                        if (completed){
                            return@Cancel
                        }
                        completed = true
                        cancels.remove(id)
                        cancel()
                    }
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
        lock.unlock()
    }
    private fun nextId(): Int {
        if (id < 2147483647){
            id++
        }else{
            id = -2147483647
        }
        return id
    }
    private var send: Send? = null
    private var id = -2147483648
    private var notifies = mutableMapOf<Int, Notify>()
    private val cancels = mutableMapOf<Int, Cancel>()
    private val replies = mutableMapOf<Int, (param: Any?, error: Any?) -> Boolean>()
    private val handles = mutableMapOf<String, Handle>()
    private val lock = ReentrantLock()
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