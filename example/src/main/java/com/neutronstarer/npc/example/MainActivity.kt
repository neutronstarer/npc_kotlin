package com.neutronstarer.npc.example

import androidx.appcompat.app.AppCompatActivity
import android.os.Bundle
import android.widget.Button
import android.widget.EditText
import android.widget.TextView
import com.neutronstarer.npc.*

import java.util.Timer
import kotlin.concurrent.schedule
class MainActivity : AppCompatActivity() {
    private var npc0 : NPC? = null
    private var npc1 : NPC? = null
    private var cancel: Cancel? = null
    private var textView: TextView? = null
    private var editTextView: EditText? = null
    private var button: Button? = null
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_main)
        textView = findViewById(R.id.textView)
        editTextView = findViewById(R.id.editTextNumber)
        button = findViewById(R.id.button)
        button?.setOnClickListener Click@{ if (cancel != null){
            cancel?.let { it() }
            cancel = null
            button?.text = "Download"
            return@Click
        }
            button?.text = "Cancel"
            var string = editTextView?.text.toString()
            string = string.ifEmpty {
                "0"
            }
            cancel = npc0?.deliver("download","/path", timeout = string.toLong(), onNotify = {param ->
                runOnUiThread {
                    textView?.text = param as CharSequence?
                }
            }, onReply = {param, error ->
                runOnUiThread{
                    if (error != null){
                        textView?.text = error as CharSequence?
                    }else{
                        textView?.text = param as CharSequence?
                    }
                    button?.text = "Download"
                    cancel = null
                }
            }) }
        npc0 = NPC()
        npc0!!.connect { message ->
            npc1?.receive(message)
        }
        npc1 = NPC()
        npc1!!.connect { message ->
            npc0?.receive(message)
        }
        config(npc0!!)
        config(npc1!!)
    }

    private fun config(npc: NPC){
        npc.on("download") Handle@{ param, reply, notify ->
            val timer = Timer()
            var i = 0
            timer.schedule(0, 1000) Timer@{
                i++
                if (i < 10) {
                    notify("progress=$i/10")
                    return@Timer
                }
                reply("did download to $param", null)
                timer.cancel()
            }
            return@Handle {
                timer.cancel()
            }
        }
    }
}