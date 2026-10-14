package com.example.bluetoothchat

import android.Manifest
import android.annotation.SuppressLint
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent
import android.bluetooth.*
import android.content.*
import android.content.pm.PackageManager
import android.os.Build
import android.os.Bundle
import android.widget.Toast
import androidx.activity.ComponentActivity
import androidx.activity.compose.setContent
import androidx.activity.result.contract.ActivityResultContracts
import androidx.compose.animation.*
import androidx.compose.foundation.background
import androidx.compose.foundation.clickable
import androidx.compose.foundation.isSystemInDarkTheme
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.foundation.shape.CircleShape
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.automirrored.filled.ArrowBack
import androidx.compose.material.icons.automirrored.filled.Send
import androidx.compose.material.icons.filled.*
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.clip
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import androidx.core.app.ActivityCompat
import androidx.core.app.NotificationCompat
import androidx.core.app.NotificationManagerCompat
import androidx.core.content.ContextCompat
import androidx.lifecycle.ViewModel
import androidx.lifecycle.ViewModelProvider
import androidx.lifecycle.viewModelScope
import androidx.room.*
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import org.json.JSONObject
import java.io.BufferedReader
import java.io.BufferedWriter
import java.io.InputStreamReader
import java.io.OutputStreamWriter
import java.text.SimpleDateFormat
import java.util.*

@Entity(tableName = "messages")
data class MessageEntity(
    @PrimaryKey val id: String = UUID.randomUUID().toString(),
    val text: String,
    val senderName: String,
    val timestamp: Long = System.currentTimeMillis(),
    val isFromMe: Boolean,
    val status: String = "SENT" 
)

@Dao
interface ChatDao {
    @Query("SELECT * FROM messages ORDER BY timestamp ASC")
    fun getAllMessages(): Flow<List<MessageEntity>>

    @Insert(onConflict = OnConflictStrategy.REPLACE)
    suspend fun insertMessage(message: MessageEntity)

    @Query("DELETE FROM messages")
    suspend fun clearAll()
}

@Database(entities = [MessageEntity::class], version = 1, exportSchema = false)
abstract class AppDatabase : RoomDatabase() {
    abstract fun chatDao(): ChatDao
    companion object {
        @Volatile private var INSTANCE: AppDatabase? = null
        fun getDatabase(context: Context): AppDatabase {
            return INSTANCE ?: synchronized(this) {
                val instance = Room.databaseBuilder(
                    context.applicationContext,
                    AppDatabase::class.java,
                    "chat_database"
                ).build()
                INSTANCE = instance
                instance
            }
        }
    }
}

data class BluetoothDeviceModel(val name: String, val address: String, val isPaired: Boolean)

enum class ConnectionState { DISCONNECTED, LISTENING, CONNECTING, CONNECTED, ERROR }

@SuppressLint("MissingPermission")
class BluetoothController(private val context: Context) {
    private val bluetoothManager = context.getSystemService(Context.BLUETOOTH_SERVICE) as BluetoothManager
    val adapter: BluetoothAdapter? = bluetoothManager.adapter
    
    private val APP_UUID = UUID.fromString("8ce255c0-200a-11e0-ac64-0800200c9a66")
    private val APP_NAME = "BluetoothChat"

    private val _connectionState = MutableStateFlow(ConnectionState.DISCONNECTED)
    val connectionState = _connectionState.asStateFlow()

    private val _scannedDevices = MutableStateFlow<List<BluetoothDeviceModel>>(emptyList())
    val scannedDevices = _scannedDevices.asStateFlow()

    private val _incomingMessages = MutableSharedFlow<MessageEntity>()
    val incomingMessages = _incomingMessages.asSharedFlow()

    private val _connectedDeviceName = MutableStateFlow<String?>(null)
    val connectedDeviceName = _connectedDeviceName.asStateFlow()

    private var serverSocket: BluetoothServerSocket? = null
    private var activeSocket: BluetoothSocket? = null
    private var writer: BufferedWriter? = null
    private val coroutineScope = CoroutineScope(Dispatchers.IO + SupervisorJob())

    private val discoveryReceiver = object : BroadcastReceiver() {
        override fun onReceive(context: Context, intent: Intent) {
            if (intent.action == BluetoothDevice.ACTION_FOUND) {
                val device = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
                    intent.getParcelableExtra(BluetoothDevice.EXTRA_DEVICE, BluetoothDevice::class.java)
                } else {
                    intent.getParcelableExtra(BluetoothDevice.EXTRA_DEVICE)
                }
                device?.let {
                    val currentList = _scannedDevices.value.toMutableList()
                    if (currentList.none { d -> d.address == it.address }) {
                        currentList.add(BluetoothDeviceModel(it.name ?: "Unknown", it.address, adapter?.bondedDevices?.contains(it) ?: false))
                        _scannedDevices.value = currentList
                    }
                }
            }
        }
    }

    init {
        context.registerReceiver(discoveryReceiver, IntentFilter(BluetoothDevice.ACTION_FOUND))
    }

    fun startDiscovery() {
        if (!hasPermissions()) return
        _scannedDevices.value = adapter?.bondedDevices?.map { BluetoothDeviceModel(it.name ?: "Unknown", it.address, true) } ?: emptyList()
        adapter?.cancelDiscovery()
        adapter?.startDiscovery()
    }

    fun stopDiscovery() {
        if (hasPermissions()) adapter?.cancelDiscovery()
    }

    fun startServerListening() {
        if (!hasPermissions() || adapter == null || !adapter.isEnabled || _connectionState.value == ConnectionState.CONNECTED) return
        coroutineScope.launch {
            try {
                _connectionState.value = ConnectionState.LISTENING
                serverSocket = adapter.listenUsingRfcommWithServiceRecord(APP_NAME, APP_UUID)
                var shouldLoop = true
                while (shouldLoop) {
                    val socket = try { serverSocket?.accept() } catch (e: Exception) { null }
                    socket?.let {
                        manageConnectedSocket(it)
                        serverSocket?.close()
                        shouldLoop = false
                    }
                }
            } catch (e: Exception) { _connectionState.value = ConnectionState.ERROR }
        }
    }

    fun connectToDevice(address: String) {
        if (!hasPermissions() || adapter == null) return
        stopDiscovery()
        coroutineScope.launch {
            try {
                _connectionState.value = ConnectionState.CONNECTING
                val socket = adapter.getRemoteDevice(address).createRfcommSocketToServiceRecord(APP_UUID)
                socket.connect()
                manageConnectedSocket(socket)
            } catch (e: Exception) {
                _connectionState.value = ConnectionState.ERROR
                delay(2000)
                startServerListening()
            }
        }
    }

    private fun manageConnectedSocket(socket: BluetoothSocket) {
        activeSocket = socket
        _connectionState.value = ConnectionState.CONNECTED
        if (hasPermissions()) _connectedDeviceName.value = socket.remoteDevice.name ?: "Device"

        coroutineScope.launch {
            try {
                writer = BufferedWriter(OutputStreamWriter(socket.outputStream, Charsets.UTF_8))
                val reader = BufferedReader(InputStreamReader(socket.inputStream, Charsets.UTF_8))
                while (isActive) {
                    reader.readLine()?.let { handleJson(it) } ?: break
                }
            } catch (e: Exception) { } finally { disconnect() }
        }
    }

    private fun handleJson(jsonStr: String) {
        try {
            val json = JSONObject(jsonStr)
            if (json.optString("type") == "MESSAGE") {
                val msg = MessageEntity(
                    id = json.getString("id"), text = json.getString("text"),
                    senderName = json.getString("senderName"), timestamp = json.getLong("timestamp"),
                    isFromMe = false, status = "RECEIVED"
                )
                coroutineScope.launch { _incomingMessages.emit(msg) }
            }
        } catch (e: Exception) { }
    }

    suspend fun sendMessage(text: String, myName: String): MessageEntity? {
        if (_connectionState.value != ConnectionState.CONNECTED || writer == null) return null
        val msg = MessageEntity(text = text, senderName = myName, isFromMe = true)
        val json = JSONObject().apply {
            put("type", "MESSAGE"); put("id", msg.id); put("text", msg.text)
            put("senderName", msg.senderName); put("timestamp", msg.timestamp)
        }
        return try {
            withContext(Dispatchers.IO) {
                writer?.write(json.toString() + "\n")
                writer?.flush()
            }
            msg
        } catch (e: Exception) { msg.copy(status = "FAILED") }
    }

    fun disconnect() {
        try { serverSocket?.close(); activeSocket?.close() } catch (e: Exception) { }
        activeSocket = null; serverSocket = null; writer = null
        _connectionState.value = ConnectionState.DISCONNECTED
        _connectedDeviceName.value = null
        startServerListening()
    }

    private fun hasPermissions(): Boolean {
        return if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.S) {
            ContextCompat.checkSelfPermission(context, Manifest.permission.BLUETOOTH_CONNECT) == PackageManager.PERMISSION_GRANTED
        } else ContextCompat.checkSelfPermission(context, Manifest.permission.ACCESS_FINE_LOCATION) == PackageManager.PERMISSION_GRANTED
    }
}

class ChatNotificationManager(private val context: Context) {
    init {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            val channel = NotificationChannel("chat_channel", "Chat", NotificationManager.IMPORTANCE_HIGH)
            (context.getSystemService(Context.NOTIFICATION_SERVICE) as NotificationManager).createNotificationChannel(channel)
        }
    }

    @SuppressLint("MissingPermission")
    fun showNotification(sender: String, message: String) {
        val intent = Intent(context, MainActivity::class.java).apply { flags = Intent.FLAG_ACTIVITY_NEW_TASK or Intent.FLAG_ACTIVITY_CLEAR_TASK }
        val pendingIntent = PendingIntent.getActivity(context, 0, intent, PendingIntent.FLAG_IMMUTABLE)
        val builder = NotificationCompat.Builder(context, "chat_channel")
            .setSmallIcon(android.R.drawable.stat_sys_data_bluetooth)
            .setContentTitle(sender).setContentText(message).setPriority(NotificationCompat.PRIORITY_HIGH)
            .setContentIntent(pendingIntent).setAutoCancel(true)

        if (ActivityCompat.checkSelfPermission(context, Manifest.permission.POST_NOTIFICATIONS) == PackageManager.PERMISSION_GRANTED || Build.VERSION.SDK_INT < Build.VERSION_CODES.TIRAMISU) {
            NotificationManagerCompat.from(context).notify(System.currentTimeMillis().toInt(), builder.build())
        }
    }
}

class ChatViewModel(
    private val database: AppDatabase, val bluetoothController: BluetoothController,
    private val notificationManager: ChatNotificationManager, private val sharedPrefs: SharedPreferences
) : ViewModel() {
    val messages = database.chatDao().getAllMessages().stateIn(viewModelScope, SharingStarted.WhileSubscribed(5000), emptyList())
    val connectionState = bluetoothController.connectionState
    val scannedDevices = bluetoothController.scannedDevices
    val connectedDeviceName = bluetoothController.connectedDeviceName
    private val _myName = MutableStateFlow(sharedPrefs.getString("display_name", "") ?: "")
    val myName = _myName.asStateFlow()

    init {
        viewModelScope.launch {
            bluetoothController.incomingMessages.collect { msg ->
                database.chatDao().insertMessage(msg)
                notificationManager.showNotification(msg.senderName, msg.text)
            }
        }
        bluetoothController.startServerListening()
    }

    fun setDisplayName(name: String) {
        sharedPrefs.edit().putString("display_name", name).apply()
        _myName.value = name
    }

    fun sendMessage(text: String) {
        if (text.isBlank()) return
        viewModelScope.launch {
            bluetoothController.sendMessage(text, myName.value)?.let { database.chatDao().insertMessage(it) }
        }
    }

    fun clearHistory() = viewModelScope.launch { database.chatDao().clearAll() }
}

class MainActivity : ComponentActivity() {
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        val db = AppDatabase.getDatabase(this)
        val btController = BluetoothController(this)
        val notifManager = ChatNotificationManager(this)
        val prefs = getSharedPreferences("ChatPrefs", Context.MODE_PRIVATE)
        val vm = ChatViewModel(db, btController, notifManager, prefs)

        setContent {
            MaterialTheme(colorScheme = if (isSystemInDarkTheme()) darkColorScheme() else lightColorScheme()) {
                Surface(modifier = Modifier.fillMaxSize()) { AppNavigation(vm) }
            }
        }
    }
}

@Composable
fun AppNavigation(viewModel: ChatViewModel) {
    var screen by remember { mutableStateOf(if (viewModel.myName.value.isEmpty()) "SETUP" else "HOME") }
    Crossfade(targetState = screen) { current ->
        when (current) {
            "SETUP" -> SetupScreen(viewModel) { screen = "HOME" }
            "HOME" -> HomeScreen(viewModel, { screen = "CHAT" }, { screen = "DISCOVER" }, { screen = "SETTINGS" })
            "DISCOVER" -> DiscoverScreen(viewModel, { screen = "HOME" }, { viewModel.bluetoothController.connectToDevice(it); screen = "CHAT" })
            "CHAT" -> ChatScreen(viewModel, { screen = "HOME" })
            "SETTINGS" -> SettingsScreen(viewModel, { screen = "HOME" })
        }
    }
}

@Composable
fun SetupScreen(viewModel: ChatViewModel, onComplete: () -> Unit) {
    var name by remember { mutableStateOf("") }
    Column(modifier = Modifier.fillMaxSize().padding(24.dp), horizontalAlignment = Alignment.CenterHorizontally, verticalArrangement = Arrangement.Center) {
        Icon(Icons.Default.BluetoothAudio, null, modifier = Modifier.size(100.dp), tint = MaterialTheme.colorScheme.primary)
        Spacer(Modifier.height(24.dp))
        Text("Bluetooth Chat", style = MaterialTheme.typography.headlineLarge, fontWeight = FontWeight.Bold)
        Spacer(Modifier.height(48.dp))
        OutlinedTextField(value = name, onValueChange = { name = it }, label = { Text("Your Display Name") }, singleLine = true, modifier = Modifier.fillMaxWidth())
        Spacer(Modifier.height(24.dp))
        Button(onClick = { viewModel.setDisplayName(name); onComplete() }, modifier = Modifier.fillMaxWidth().height(50.dp), enabled = name.isNotBlank()) { Text("Continue") }
    }
}

@Composable
fun HomeScreen(viewModel: ChatViewModel, onChat: () -> Unit, onDiscover: () -> Unit, onSettings: () -> Unit) {
    val state by viewModel.connectionState.collectAsState()
    val context = LocalContext.current
    val launcher = rememberLauncherForActivityResult(ActivityResultContracts.RequestMultiplePermissions()) { perms -> if (perms.all { it.value }) onDiscover() }

    Scaffold(
        topBar = { @OptIn(ExperimentalMaterial3Api::class) TopAppBar(title = { Text("Bluetooth Chat") }, actions = { IconButton(onClick = onSettings) { Icon(Icons.Default.Settings, null) } }) },
        floatingActionButton = {
            FloatingActionButton(onClick = {
                val req = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.S) arrayOf(Manifest.permission.BLUETOOTH_SCAN, Manifest.permission.BLUETOOTH_CONNECT, Manifest.permission.POST_NOTIFICATIONS) else arrayOf(Manifest.permission.ACCESS_FINE_LOCATION)
                if (req.all { ContextCompat.checkSelfPermission(context, it) == PackageManager.PERMISSION_GRANTED }) onDiscover() else launcher.launch(req)
            }) { Icon(Icons.Default.Search, null) }
        }
    ) { p ->
        Column(Modifier.padding(p).fillMaxSize()) {
            Card(modifier = Modifier.fillMaxWidth().padding(16.dp).clickable { if(state == ConnectionState.CONNECTED) onChat() }) {
                Row(Modifier.padding(16.dp), verticalAlignment = Alignment.CenterVertically) {
                    Icon(if (state == ConnectionState.CONNECTED) Icons.Default.BluetoothConnected else Icons.Default.Bluetooth, null)
                    Spacer(Modifier.width(16.dp))
                    Text(state.name, fontWeight = FontWeight.Bold)
                }
            }
            ListItem(headlineContent = { Text("Message History") }, leadingContent = { Icon(Icons.Default.History, null) }, modifier = Modifier.clickable { onChat() })
        }
    }
}

@Composable
fun DiscoverScreen(viewModel: ChatViewModel, onBack: () -> Unit, onSelect: (String) -> Unit) {
    val devices by viewModel.scannedDevices.collectAsState()
    DisposableEffect(Unit) { viewModel.bluetoothController.startDiscovery(); onDispose { viewModel.bluetoothController.stopDiscovery() } }
    Scaffold(topBar = { @OptIn(ExperimentalMaterial3Api::class) TopAppBar(title = { Text("Nearby Devices") }, navigationIcon = { IconButton(onClick = onBack) { Icon(Icons.AutoMirrored.Filled.ArrowBack, null) } }) }) { p ->
        LazyColumn(Modifier.padding(p).fillMaxSize()) {
            items(devices) { d ->
                ListItem(headlineContent = { Text(d.name) }, supportingContent = { Text(d.address) }, modifier = Modifier.clickable { onSelect(d.address) })
                HorizontalDivider()
            }
        }
    }
}

@Composable
fun ChatScreen(viewModel: ChatViewModel, onBack: () -> Unit) {
    val messages by viewModel.messages.collectAsState()
    val state by viewModel.connectionState.collectAsState()
    var text by remember { mutableStateOf("") }
    val formatter = remember { SimpleDateFormat("hh:mm a", Locale.getDefault()) }

    Scaffold(
        topBar = { @OptIn(ExperimentalMaterial3Api::class) TopAppBar(title = { Text("Chat - ${state.name}") }, navigationIcon = { IconButton(onClick = onBack) { Icon(Icons.AutoMirrored.Filled.ArrowBack, null) } }, actions = { IconButton(onClick = { viewModel.bluetoothController.disconnect() }) { Icon(Icons.Default.BluetoothDisabled, null, tint = Color.Red) } }) },
        bottomBar = {
            Row(Modifier.fillMaxWidth().padding(8.dp), verticalAlignment = Alignment.CenterVertically) {
                OutlinedTextField(value = text, onValueChange = { text = it }, modifier = Modifier.weight(1f), shape = RoundedCornerShape(24.dp))
                Spacer(Modifier.width(8.dp))
                FloatingActionButton(onClick = { viewModel.sendMessage(text); text = "" }, modifier = Modifier.size(50.dp), shape = CircleShape) { Icon(Icons.AutoMirrored.Filled.Send, null) }
            }
        }
    ) { p ->
        LazyColumn(Modifier.padding(p).fillMaxSize().padding(8.dp), reverseLayout = true) {
            items(messages.reversed()) { msg ->
                Row(Modifier.fillMaxWidth().padding(vertical = 4.dp), horizontalArrangement = if (msg.isFromMe) Arrangement.End else Arrangement.Start) {
                    Column(Modifier.clip(RoundedCornerShape(16.dp)).background(if (msg.isFromMe) MaterialTheme.colorScheme.primaryContainer else MaterialTheme.colorScheme.secondaryContainer).padding(12.dp).widthIn(max = 280.dp)) {
                        if (!msg.isFromMe) Text(msg.senderName, fontSize = 12.sp, color = MaterialTheme.colorScheme.primary, fontWeight = FontWeight.Bold)
                        Text(msg.text, fontSize = 16.sp)
                        Text(formatter.format(Date(msg.timestamp)), fontSize = 10.sp, color = Color.Gray, modifier = Modifier.align(Alignment.End))
                    }
                }
            }
        }
    }
}

@Composable
fun SettingsScreen(viewModel: ChatViewModel, onBack: () -> Unit) {
    Scaffold(topBar = { @OptIn(ExperimentalMaterial3Api::class) TopAppBar(title = { Text("Settings") }, navigationIcon = { IconButton(onClick = onBack) { Icon(Icons.AutoMirrored.Filled.ArrowBack, null) } }) }) { p ->
        Column(Modifier.padding(p)) {
            ListItem(headlineContent = { Text("Clear History", color = Color.Red) }, leadingContent = { Icon(Icons.Default.Delete, null, tint = Color.Red) }, modifier = Modifier.clickable { viewModel.clearHistory(); onBack() })
        }
    }
}
