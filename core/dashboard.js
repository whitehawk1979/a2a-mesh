
var ws = null;
var nodeId = "";
var authToken = null;
var authUser = null;
var isAdmin = false;
var messageHistory = [];
var currentChannel = "general";  // "general" or agent name for DM
var channelMessages = {};  // channel -> [messages]
channelMessages["general"] = [];
var openChats = ["general"];  // track which chats are open as tabs

// ─── Open Chats Bar — horizontal tabs above send area ───
function renderOpenChatsBar() {
  var bar = document.getElementById("openChatsBar");
  if (!bar) return;
  bar.innerHTML = "";
  openChats.forEach(function(ch) {
    var isGeneral = (ch === "general");
    var isActive = (ch === currentChannel);
    var status = knownAgents[ch] || (isGeneral ? "online" : "offline");
    var dot = isGeneral ? "" : (status === "online") ? "🟢 " : (status === "available" || status === "connected") ? "🟡 " : "🔴 ";
    var tab = document.createElement("div");
    tab.style.cssText = "display:flex;align-items:center;gap:4px;padding:4px 10px;border-radius:8px;font-size:12px;cursor:pointer;white-space:nowrap;flex-shrink:0;" +
      (isActive ? "background:var(--primary);color:#fff;font-weight:600;" : "background:var(--surface2);color:var(--text);border:1px solid var(--border);");
    var label = document.createElement("span");
    label.textContent = (isGeneral ? "💬 " : dot + "👤 ") + (isGeneral ? "Közös" : ch);
    label.onclick = function() { switchChannel(ch); };
    tab.appendChild(label);
    // Close button (not for general)
    if (!isGeneral) {
      var closeBtn = document.createElement("span");
      closeBtn.textContent = "✕";
      closeBtn.style.cssText = "margin-left:4px;opacity:" + (isActive ? "0.9" : "0.5") + ";font-size:11px;padding:0 3px;border-radius:4px;";
      closeBtn.onmouseover = function() { this.style.opacity = "1"; this.style.background = "rgba(255,0,0,0.2)"; };
      closeBtn.onmouseout = function() { this.style.opacity = isActive ? "0.9" : "0.5"; this.style.background = ""; };
      closeBtn.onclick = function(e) {
        e.stopPropagation();
        closeChat(ch);
      };
      tab.appendChild(closeBtn);
    }
    bar.appendChild(tab);
  });
}

function openChat(channel) {
  if (openChats.indexOf(channel) === -1) {
    openChats.push(channel);
  }
  renderOpenChatsBar();
}

function closeChat(channel) {
  var idx = openChats.indexOf(channel);
  if (idx === -1) return;
  openChats.splice(idx, 1);
  // If closing the active channel, switch to the last remaining one
  if (currentChannel === channel) {
    var next = openChats[openChats.length - 1] || "general";
    if (openChats.indexOf("general") === -1) openChats.unshift("general");
    switchChannel(next);
  } else {
    renderOpenChatsBar();
  }
}

function toggleSidebar() {
  var sb = document.getElementById("channelSidebar");
  sb.classList.toggle("open");
  var ov = document.getElementById("sidebarOverlay");
  if (sb.classList.contains("open")) {
    if (!ov) { ov = document.createElement("div"); ov.id = "sidebarOverlay"; ov.className = "sidebar-overlay"; ov.onclick = toggleSidebar; document.body.appendChild(ov); }
    ov.style.display = "block";
  } else {
    if (ov) ov.style.display = "none";
  }
  // Update active bottom nav
  updateMobileNav("channels");
}

function toggleInfoPanel() {
  var panel = document.getElementById("mobileInfoPanel");
  panel.classList.toggle("open");
  var ov = document.getElementById("infoPanelOverlay");
  if (panel.classList.contains("open")) {
    ov.style.display = "block";
    // Clone right panel content into mobile panel
    syncMobileInfoPanel();
  } else {
    ov.style.display = "none";
  }
  updateMobileNav("info");
}

function syncMobileInfoPanel() {
  var rp = document.querySelector(".right-panel");
  var mp = document.getElementById("mobileInfoPanelContent");
  if (rp && mp) {
    mp.innerHTML = rp.innerHTML;
  }
}

function switchMobileTab(tab) {
  updateMobileNav(tab);
  if (tab === "chat") {
    // Close sidebar and info panel, show chat
    var sb = document.getElementById("channelSidebar");
    sb.classList.remove("open");
    var sov = document.getElementById("sidebarOverlay");
    if (sov) sov.style.display = "none";
    var ip = document.getElementById("mobileInfoPanel");
    ip.classList.remove("open");
    var iov = document.getElementById("infoPanelOverlay");
    if (iov) iov.style.display = "none";
  } else if (tab === "channels") {
    toggleSidebar();
  } else if (tab === "info") {
    toggleInfoPanel();
  } else if (tab === "nodes") {
    // Open sidebar and scroll to nodes section
    var sb = document.getElementById("channelSidebar");
    if (!sb.classList.contains("open")) toggleSidebar();
    setTimeout(function() {
      var ns = document.getElementById("allNodesList");
      if (ns) ns.scrollIntoView({behavior: "smooth", block: "start"});
    }, 350);
  } else if (tab === "lab") {
    // Lab: show in modal iframe (unified UI)
    var m = document.getElementById('labModal');
    m.style.display = 'flex';
    document.getElementById('labFrame').src = '/lab?t=' + Date.now();
  } else if (tab === "kanban") {
    showDelegations();
  } else if (tab === "marveen") {
    // Marveen: show in modal iframe (unified UI)
    var m = document.getElementById('marveenModal');
    m.style.display = 'flex';
    document.getElementById('marveenFrame').src = '/marveen?t=' + Date.now();
  }
}

function updateMobileNav(active) {
  document.querySelectorAll(".mobile-bottom-nav .nav-item").forEach(function(el) { el.classList.remove("active"); });
  var map = {chat: "navChat", channels: "navChannels", info: "navInfo", nodes: "navNodes", topology: "navTopology", skills: "navSkills", workflows: "navWorkflows", alerts: "navAlerts", lab: "navLab", kanban: "navKanban", marveen: "navMarveen", settings: "navSettings"};
  var el = document.getElementById(map[active] || "navChat");
  if (el) el.classList.add("active");
}

function switchChannel(channel) {
  currentChannel = channel;
  // Track as open chat tab
  openChat(channel);
  // Update UI
  document.querySelectorAll(".channel-item").forEach(function(el) { el.classList.remove("active"); });
  var chEl = document.getElementById("ch-" + channel);
  if (chEl) chEl.classList.add("active");

  // Close sidebar on mobile after channel switch
  var sb = document.getElementById("channelSidebar");
  if (sb.classList.contains("open")) {
    sb.classList.remove("open");
    var sov = document.getElementById("sidebarOverlay");
    if (sov) sov.style.display = "none";
  }
  updateMobileNav("chat");

  // Update header
  var header = document.getElementById("chatChannel");
  var info = document.getElementById("chatInfo");
  if (channel === "general") {
    header.innerHTML = "💬 Közös Szoba";
    info.innerHTML = "Minden agent látja és válaszol — valós idejű közös chat";
    document.getElementById("recipientSelect").value = "";
    // Hide DM agent indicator in general channel
    var ind = document.getElementById("dmAgentIndicator");
    if (ind) ind.style.display = "none";
  } else {
    header.innerHTML = "👤 " + channel;
    info.innerHTML = "Közvetlen üzenet — csak te és " + channel;
    // Set recipient to this agent — use .value (more reliable than selectedIndex)
    var selEl = document.getElementById("recipientSelect");
    if (selEl) selEl.value = channel;
    // Fallback: if .value didn't stick (option not found), find by text
    if (selEl && selEl.value !== channel) {
      var opts = selEl.options;
      for (var i = 0; i < opts.length; i++) {
        if (opts[i].value === channel) { selEl.selectedIndex = i; break; }
      }
    }
    // Show DM agent indicator with status
    var status = knownAgents[channel] || "offline";
    var dot = (status === "online") ? "🟢" : (status === "available" || status === "connected") ? "🟡" : "🔴";
    var ind = document.getElementById("dmAgentIndicator");
    var indDot = document.getElementById("dmAgentDot");
    var indName = document.getElementById("dmAgentName");
    if (ind && indDot && indName) {
      indDot.textContent = dot;
      indName.textContent = channel + (status === "online" ? " (aktív)" : status === "available" || status === "connected" ? " (elérhető)" : " (offline)");
      ind.style.display = "flex";
    }
  }

  // For DM channels: skip renderChannelMessages (it uses channelMessages which only has sent msgs)
  // and load directly from server via _loadChatMessages (Marveen conversation pattern)
  var ch = currentChannel || "general";
  if (ch !== "general" && typeof window._loadChatMessages === "function") {
    // Clear and show loading state
    var container = document.getElementById("messages");
    if (container) container.innerHTML = '<div style="color:var(--text3);font-size:13px;text-align:center;padding:20px;">Töltés...</div>';
    // Load DM messages immediately
    window._loadChatMessages(ch, false);
  } else {
    // General channel: render from channelMessages cache
    renderChannelMessages();
  }
  // Update open chats bar (highlight active tab)
  renderOpenChatsBar();

  // Also poll for new messages (loadMessages routes DM channels to _loadChatMessages)
  loadMessages();
}

function renderChannelMessages() {
  var container = document.getElementById("messages");
  container.innerHTML = "";
  // Add "load more" indicator at top if there's more history
  if (window._chatHasMore && window._chatOldestId) {
    var indicator = document.createElement("div");
    indicator.id = "load-more-indicator";
    indicator.style.cssText = "text-align:center;padding:8px;color:#888;font-size:12px;cursor:pointer;";
    indicator.textContent = "↑ Régebbi üzenetek betöltése…";
    indicator.onclick = function() { loadOlderMessages(); };
    container.appendChild(indicator);
  }
  var msgs = channelMessages[currentChannel] || [];
  msgs.forEach(function(m) { addMessageToDOM(m, false); });
  scrollToBottom();
}

// ── Lazy-load older messages on scroll-up ──
var _isLoadingOlder = false;

function loadOlderMessages() {
  if (_isLoadingOlder || !window._chatHasMore || !window._chatOldestId) return;
  _isLoadingOlder = true;
  var container = document.getElementById("messages");
  var indicator = document.getElementById("load-more-indicator");
  if (indicator) indicator.textContent = "Betöltés…";

  // Save scroll position for restoration after prepend
  var prevScrollHeight = container.scrollHeight;
  var prevScrollTop = container.scrollTop;

  var ch = currentChannel || "general";
  var url = "/api/chat/messages?limit=30&before_id=" + window._chatOldestId;
  if (ch !== "general") {
    url = "/api/chat/messages?with=" + encodeURIComponent(ch) + "&limit=30&before_id=" + window._chatOldestId;
  }

  fetch(url).then(function(r) { return r.json(); }).then(function(d) {
    if (!d || !d.messages) { _isLoadingOlder = false; return; }
    // Messages come in DESC order (newest first) — reverse to chronological
    var olderMsgs = d.messages.reverse();
    // Prepend to channelMessages
    var existing = channelMessages[ch] || [];
    // Dedup by id
    var existingIds = {};
    existing.forEach(function(m) { existingIds[m.id] = true; });
    var newMsgs = [];
    olderMsgs.forEach(function(m) {
      m.content = m.content || m.text || "";
      m.timestamp = m.timestamp || m.created_at || "";
      m.type = m.type || m.msg_type || "";
      if (!existingIds[m.id]) newMsgs.push(m);
    });
    channelMessages[ch] = newMsgs.concat(existing);

    // Update pagination state
    window._chatHasMore = d.has_more || false;
    window._chatOldestId = d.oldest_id || window._chatOldestId;

    // Re-render with scroll position preserved
    container.innerHTML = "";
    if (window._chatHasMore && window._chatOldestId) {
      var ind = document.createElement("div");
      ind.id = "load-more-indicator";
      ind.style.cssText = "text-align:center;padding:8px;color:#888;font-size:12px;cursor:pointer;";
      ind.textContent = "↑ Régebbi üzenetek betöltése…";
      ind.onclick = function() { loadOlderMessages(); };
      container.appendChild(ind);
    }
    channelMessages[ch].forEach(function(m) { addMessageToDOM(m, false); });

    // Restore scroll position (keep user at same visual position)
    var newScrollHeight = container.scrollHeight;
    container.scrollTop = prevScrollTop + (newScrollHeight - prevScrollHeight);

    _isLoadingOlder = false;
  }).catch(function(e) {
    console.warn("[Chat] loadOlderMessages error:", e);
    _isLoadingOlder = false;
    if (indicator) indicator.textContent = "↑ Régebbi üzenetek betöltése…";
  });
}

// Auto-trigger on scroll-up near top
function _initChatScrollListener() {
  var container = document.getElementById("messages");
  if (!container) return;
  container.addEventListener("scroll", function() {
    if (container.scrollTop < 50 && window._chatHasMore && !_isLoadingOlder) {
      loadOlderMessages();
    }
  });
}

function addMessageToDOM(msg, scroll) {
  if (scroll === undefined) scroll = true;
  // Remove any 'processing' indicator from the same agent when a real reply arrives
  if (msg.type !== "agent_processing" && msg.type !== "agent_timeout") {
    var processingEls = document.querySelectorAll('[id^="msg-processing_"]');
    processingEls.forEach(function(el) { el.remove(); });
    // Also remove any timeout messages
    var timeoutEls = document.querySelectorAll('[id^="msg-timeout_"]');
    timeoutEls.forEach(function(el) { el.remove(); });
    messageHistory = messageHistory.filter(function(m) { return m.type !== "agent_processing" && m.type !== "agent_timeout"; });
  }
  var myUsername = (authUser ? authUser.username : localStorage.getItem("a2a_username")) || "zsolt";
  var myDisplay = authUser ? authUser.display_name : "";
  var isSent = msg.sender === myUsername || msg.sender === myDisplay || msg.sender === nodeId;
  // DM: recipient is a specific agent (not broadcast, not me)
  var msgType = msg.type || msg.msg_type || "";
  var isDM = msg.recipient && msg.recipient !== "broadcast" && msg.recipient !== myUsername && msgType !== "agent_reply";
  // Agent replies in a DM context go to the DM channel with that agent
  if (msgType === "agent_reply" && msg.sender && msg.sender !== "broadcast" && msg.sender !== myUsername) {
    isDM = true;
  }
  var isBroadcast = !isDM && (!msg.recipient || msg.recipient === "broadcast" || msg.type === "agent_reply" || msg.type === "directive");

  var cls = isSent ? "sent" : (msg.type === "agent_processing" ? "system" : (isDM ? "dm" : (isBroadcast ? "broadcast" : "received")));
  var pri = msg.priority || 5;
  var priCls = pri >= 7 ? "p-high" : pri >= 4 ? "p-med" : "p-low";
  var priLabel = pri >= 7 ? "SÜRGŐS" : pri >= 4 ? "normál" : "alacsony";
  var senderLabel = msg.sender || msg.username || "?";
  var time = msg.timestamp ? new Date(msg.timestamp).toLocaleTimeString("hu-HU") : new Date().toLocaleTimeString("hu-HU");

  // Determine sender class
  var senderCls = "agent";
  if (isSent) senderCls = "self";
  else if (msg.source === "web_dashboard") senderCls = "owner";

  var recipientLabel = "";
  if (isBroadcast) recipientLabel = "";
  else if (isDM) recipientLabel = " → " + escapeHtml(msg.recipient);
  else recipientLabel = " → " + escapeHtml(msg.recipient);

  // Format content — strip JSON wrappers, show only the text
  var displayContent = msg.content || "";
  var contentHtml;

  // If content looks like JSON, try to extract just the "text" field
  if (displayContent.trim().startsWith("{") && displayContent.trim().endsWith("}")) {
    try {
      var parsed = JSON.parse(displayContent);
      // If it has a "text" field, use that as the display content
      if (parsed.text && typeof parsed.text === "string") {
        displayContent = parsed.text;
        contentHtml = escapeHtml(displayContent);
      } else if (parsed.content && typeof parsed.content === "string") {
        displayContent = parsed.content;
        contentHtml = escapeHtml(displayContent);
      } else if (parsed.response && typeof parsed.response === "string") {
        displayContent = parsed.response;
        contentHtml = escapeHtml(displayContent);
      } else if (parsed.message && typeof parsed.message === "string") {
        displayContent = parsed.message;
        contentHtml = escapeHtml(displayContent);
      } else {
        // No recognized text field — show as simple key-value, but only meaningful fields
        var parts = [];
        for (var k in parsed) {
          if (parsed.hasOwnProperty(k)) {
            var v = parsed[k];
            var vStr = typeof v === "object" ? JSON.stringify(v) : String(v);
            if (vStr.length > 80) vStr = vStr.substring(0, 77) + "...";
            parts.push('<span style="color:var(--accent)">' + escapeHtml(k) + '</span>: ' + escapeHtml(vStr));
          }
        }
        contentHtml = parts.join('<br>');
      }
    } catch(e) {
      contentHtml = escapeHtml(displayContent);
    }
  } else {
    contentHtml = escapeHtml(displayContent);
  }

  var div = document.createElement("div");
  div.className = "msg " + cls;
  div.id = "msg-" + (msg.id || "");
  var _fileContent = "";
  if (msg.attachment || (msg.type === "file" && msg.file_url)) {
    var _att = msg.attachment || { file_name: msg.content, mime_type: msg.mime_type || "", file_size: 0, url: msg.file_url || "" };
    _fileContent = window._fileCardHtml(_att, isSent);
  }
  div.innerHTML =
    '<div class="sender ' + senderCls + '">' + escapeHtml(senderLabel) + recipientLabel +
      (isAdmin ? ' <button onclick="deleteMessage(\'' + (msg.id || "") + '\')" style="float:right;background:none;border:none;color:#ff5c5c;cursor:pointer;font-size:12px;" title="Törlés">✕</button>' : '') +
    '</div>' +
    '<div>' + contentHtml + _fileContent + '</div>' +
    '<div class="meta">' +
      '<span>' + time + '</span>' +
      '<span class="priority ' + priCls + '">' + priLabel + '</span>' +
      (msg.source === "web_dashboard" ? '<span>🌐</span>' : '<span>🤖</span>') +
    '</div>';
  document.getElementById("messages").appendChild(div);
  messageHistory.push(msg);
  if (messageHistory.length > 500) messageHistory.shift();
  if (scroll) scrollToBottom();
}

function addMessage(msg, scroll) {
  if (scroll === undefined) scroll = true;

  // ─── Dedup: skip if message with same ID already exists in ANY channel ───
  if (msg.id) {
    for (var ch in channelMessages) {
      for (var i = 0; i < channelMessages[ch].length; i++) {
        if (channelMessages[ch][i].id === msg.id) {
          return;  // Already have this message
        }
      }
    }
  }
  // Also check local-only messages (id starts with "local_") by content+sender+time window
  if (msg.content && msg.sender) {
    var now = Date.now();
    var msgTime = msg.timestamp ? new Date(msg.timestamp).getTime() : now;
    for (var ch2 in channelMessages) {
      for (var j = 0; j < channelMessages[ch2].length; j++) {
        var existing = channelMessages[ch2][j];
        if (existing.sender === msg.sender &&
            existing.content === msg.content &&
            Math.abs((existing.timestamp ? new Date(existing.timestamp).getTime() : 0) - msgTime) < 5000) {
          return;  // Same content from same sender within 5 seconds = duplicate
        }
      }
    }
  }

  // Route message to the correct channel
  var myUsername = (authUser ? authUser.username : localStorage.getItem("a2a_username")) || "zsolt";
  var myDisplay = authUser ? authUser.display_name : "";
  var msgType = msg.type || msg.msg_type || "";
  var isDM = msg.recipient && msg.recipient !== "broadcast" && msg.recipient !== myUsername;
  // Agent replies in a DM context go to the DM channel with that agent
  if (msgType === "agent_reply" && msg.sender && msg.sender !== "broadcast" && msg.sender !== myUsername) {
    isDM = true;
  }
  // Directives from web_dashboard with a specific recipient are DMs
  if (!isDM && msgType === "directive" && msg.recipient && msg.recipient !== "broadcast" && msg.source === "web_dashboard") {
    isDM = true;
  }
  var channel;

  if (isDM) {
    // DM message — route to the DM channel with the other party
    if (msg.sender === myUsername || msg.sender === (authUser ? authUser.display_name : "") || msg.sender === nodeId) {
      channel = msg.recipient;  // I sent it → show in DM with recipient
    } else {
      channel = msg.sender;  // Someone sent it to me → show in DM with sender
    }
  } else {
    channel = "general";  // Broadcast → general channel
  }

  if (!channelMessages[channel]) channelMessages[channel] = [];
  channelMessages[channel].push(msg);
  if (channelMessages[channel].length > 200) channelMessages[channel].shift();

  // Show in current view if it matches current channel
  if (channel === currentChannel) {
    addMessageToDOM(msg, scroll);
  } else {
    // Auto-open chat tab for this channel
    openChat(channel);
    // Show notification badge on the channel
    var chEl = document.getElementById("ch-" + channel);
    if (!chEl) {
      // Create DM channel entry
      addDMChannel(channel);
      chEl = document.getElementById("ch-" + channel);
    }
    if (chEl) {
      var badge = chEl.querySelector(".badge");
      if (!badge) {
        badge = document.createElement("span");
        badge.className = "badge";
        chEl.appendChild(badge);
      }
      var count = parseInt(badge.textContent || "0") + 1;
      badge.textContent = count;
    }
    // Also show broadcast messages in general even if viewing DM
    if (!isDM && currentChannel !== "general") {
      if (!channelMessages["general"]) channelMessages["general"] = [];
      channelMessages["general"].push(msg);
    }
  }
}

var knownAgents = {};  // agent name -> status info for DM channel status dots

function addDMChannel(agentName, status) {
  // Track agent status
  if (status) knownAgents[agentName] = status;
  var list = document.getElementById("dmList");
  if (!list) return;
  var existing = document.getElementById("ch-" + agentName);
  var statusInfo = knownAgents[agentName] || "offline";
  var statusIcon = (statusInfo === "online") ? "🟢" : (statusInfo === "available" || statusInfo === "connected") ? "🟡" : "🔴";
  if (existing) {
    // Update status dot in existing channel item
    var iconSpan = existing.querySelector(".dm-status-dot");
    if (iconSpan) iconSpan.textContent = statusIcon;
    // Update DM agent indicator if this is the current channel
    if (currentChannel === agentName) {
      var indDot = document.getElementById("dmAgentDot");
      var indName = document.getElementById("dmAgentName");
      if (indDot && indName) {
        indDot.textContent = statusIcon;
        indName.textContent = agentName + (statusInfo === "online" ? " (aktív)" : statusInfo === "available" || statusInfo === "connected" ? " (elérhető)" : " (offline)");
      }
    }
    return;
  }
  var div = document.createElement("div");
  div.className = "channel-item";
  div.id = "ch-" + agentName;
  div.onclick = function() { switchChannel(agentName); };
  div.innerHTML = '<span class="dm-status-dot">' + statusIcon + '</span> <span class="icon">👤</span> ' + escapeHtml(agentName);
  list.appendChild(div);
}

function sendMessage() {
  var input = document.getElementById("messageInput");
  var content = input.value.trim();
  if (!content) return;
  var to = document.getElementById("recipientSelect").value || "broadcast";
  var priority = parseInt(document.getElementById("prioritySelect").value) || 5;

  // Pre-flight token check
  var token = localStorage.getItem("a2a_token") || localStorage.getItem("mesh_token") || "";
  if (!token) {
    log("No token — showing login modal");
    if (typeof showAuth === "function") { showAuth(); }
    else { alert("Bejelentkezés szükséges! Kattints a bejelentkezés gombra."); }
    return;
  }

  // Use /api/chat/send which handles PG storage, auto-ack, and mesh routing
  fetch("/api/chat/send", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Authorization": "Bearer " + token
    },
    body: JSON.stringify({ recipient: to, content: content, msg_type: "chat" })
  }).then(function(r) {
    if (r.status === 401) {
      log("Token expired — showing login modal");
      localStorage.removeItem("a2a_token");
      localStorage.removeItem("mesh_token");
      if (typeof showAuth === "function") { showAuth(); }
      else { alert("Lejárt a session! Jelentkezz be újra."); }
      return null;
    }
    return r.json();
  }).then(function(d) {
    if (!d) return;
    if (d.ok) {
      input.value = "";
      scrollToBottom();
      // For DM channels: use _loadChatMessages (Marveen pattern — single conversation view)
      // For general: use addMessage + loadMessages
      var ch = currentChannel || "general";
      if (ch !== "general") {
        // DM: instant refresh via dedicated loader
        setTimeout(function() { window._loadChatMessages(ch, true); }, 100);
        setTimeout(function() { window._loadChatMessages(ch, true); }, 1000);
        setTimeout(function() { window._loadChatMessages(ch, true); }, 3000);
      } else {
        // Broadcast: add to main chat and refresh
        addMessage({
          id: d.message_id || ("local_" + Date.now()),
          sender: (authUser ? authUser.display_name : "Zsolt") || nodeId,
          recipient: to,
          type: "chat",
          content: content,
          timestamp: new Date().toISOString(),
          priority: priority,
          source: "web_dashboard",
          username: authUser ? authUser.display_name : "Zsolt"
        }, true);
        scrollToBottom();
        setTimeout(loadMessages, 300);
        setTimeout(loadMessages, 1500);
      }
    } else {
      log("Send failed: " + (d.error || "unknown"));
      alert("Küldés sikertelen: " + (d.error || "ismeretlen hiba"));
    }
  }).catch(function(e) {
    log("Send error: " + e);
    alert("Küldés hiba: " + e.message);
  });
}

// ─────────────────────────────────────────────────────────
// ── Telegram-style typing indicator (agent thinking/writing) ──
// Shows in BOTH the general room and the active DM view.
window._typingTimers = {};

window.showTypingIndicator = function(agent, chatType) {
  var container = document.getElementById("messages") || document.getElementById("chatMessages");
  if (!container) return;
  var tid = "typing-" + agent;
  var existing = document.getElementById(tid);
  if (existing) existing.remove();
  var div = document.createElement("div");
  div.className = "typing-indicator";
  div.id = tid;
  var label = "gondolkodik…";
  div.innerHTML =
    '<div class="typing-agent">' + escapeHtml(agent) + '</div>' +
    '<div class="typing-dots"><span></span><span></span><span></span></div>' +
    '<div class="typing-label">' + label + '</div>';
  container.appendChild(div);
  container.scrollTop = container.scrollHeight;
  // Auto-remove after 120s (safety — in case the stop event is missed)
  if (window._typingTimers[agent]) clearTimeout(window._typingTimers[agent]);
  window._typingTimers[agent] = setTimeout(function() { window.hideTypingIndicator(agent); }, 260000);
};

window.hideTypingIndicator = function(agent) {
  var el = document.getElementById("typing-" + agent);
  if (el) el.remove();
  if (window._typingTimers[agent]) { clearTimeout(window._typingTimers[agent]); delete window._typingTimers[agent]; }
};

window.hideAllTypingIndicators = function() {
  var els = document.querySelectorAll(".typing-indicator");
  els.forEach(function(el) { el.remove(); });
  for (var k in window._typingTimers) { clearTimeout(window._typingTimers[k]); }
  window._typingTimers = {};
};

// ─────────────────────────────────────────────────────────
// ── File attachment rendering + preview modal (chat) ──

window._fileIcon = function(mime, name) {
  var m = (mime || '').toLowerCase();
  if (m.startsWith('image/')) return '🖼️';
  if (m.startsWith('audio/')) return '🎵';
  if (m.startsWith('video/')) return '🎬';
  if (m.indexOf('pdf') >= 0) return '📄';
  if (m.indexOf('word') >= 0 || m.indexOf('document') >= 0) return '📝';
  if (m.indexOf('sheet') >= 0 || m.indexOf('excel') >= 0) return '📊';
  if (m.indexOf('zip') >= 0 || m.indexOf('compress') >= 0 || m.indexOf('tar') >= 0) return '📦';
  if (m.indexOf('json') >= 0 || m.indexOf('text') >= 0 || /\.(txt|md|csv|log|py|js|yaml|yml)$/i.test(name || '')) return '📄';
  return '📎';
};

window._formatFileSize = function(bytes) {
  if (!bytes) return '';
  if (bytes < 1024) return bytes + ' B';
  if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + ' KB';
  return (bytes / (1024 * 1024)).toFixed(1) + ' MB';
};

window._fileCardHtml = function(att, isSent) {
  var icon = window._fileIcon(att.mime_type, att.file_name);
  var size = window._formatFileSize(att.file_size);
  var bg = isSent ? 'rgba(255,255,255,0.15)' : 'var(--surface2)';
  var border = isSent ? 'rgba(255,255,255,0.3)' : 'var(--border)';
  // Auth via query token — img/iframe/a tags can't send Authorization headers
  var _tok = localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || '';
  var _url = att.url || '';
  if (_url && _tok) _url += (_url.indexOf('?') >= 0 ? '&' : '?') + 'token=' + encodeURIComponent(_tok);
  att = Object.assign({}, att, { url: _url });
  var html = '<div style="display:flex;align-items:center;gap:10px;background:' + bg + ';border:1px solid ' + border + ';border-radius:10px;padding:8px 12px;margin-top:4px;cursor:pointer;max-width:100%;" onclick="openFilePreviewModal(\'' + encodeURIComponent(JSON.stringify(att)) + '\')">';
  html += '<span style="font-size:22px;">' + icon + '</span>';
  html += '<div style="flex:1;min-width:0;">';
  html += '<div style="font-size:12px;font-weight:600;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">' + esc(att.file_name) + '</div>';
  html += '<div style="font-size:10px;opacity:0.6;">' + esc(size) + ' • kattints a megnyitáshoz</div>';
  html += '</div>';
  html += '<span style="font-size:16px;opacity:0.6;">👁️</span>';
  html += '</div>';
  return html;
};

window.openFilePreviewModal = function(encodedAtt) {
  var att;
  try { att = JSON.parse(decodeURIComponent(encodedAtt)); } catch (e) { return; }
  var overlay = document.getElementById('filePreviewOverlay');
  if (overlay) overlay.remove();
  overlay = document.createElement('div');
  overlay.id = 'filePreviewOverlay';
  overlay.style.cssText = 'position:fixed;inset:0;background:rgba(0,0,0,0.75);z-index:99999;display:flex;align-items:center;justify-content:center;padding:20px;';
  overlay.onclick = function(e) { if (e.target === overlay) overlay.remove(); };

  var mime = (att.mime_type || '').toLowerCase();
  var url = att.url || '';
  var token = localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || '';
  var dlUrl = url + (url.indexOf('?') >= 0 ? '&' : '?') + 'download=1';

  var content = '';
  if (mime.startsWith('image/')) {
    content = '<img src="' + esc(url) + '" style="max-width:100%;max-height:60vh;border-radius:10px;display:block;margin:0 auto;" />';
  } else if (mime.startsWith('video/')) {
    content = '<video src="' + esc(url) + '" controls style="max-width:100%;max-height:60vh;border-radius:10px;display:block;margin:0 auto;"></video>';
  } else if (mime.startsWith('audio/')) {
    content = '<div style="text-align:center;font-size:48px;padding:20px;">🎵</div><audio src="' + esc(url) + '" controls style="width:100%;display:block;margin-top:12px;"></audio>';
  } else {
    // Document: inline iframe preview for text/pdf-like, else icon
    if (mime.indexOf('pdf') >= 0 || mime.indexOf('text') >= 0 || mime.indexOf('json') >= 0) {
      content = '<iframe src="' + esc(url) + '" style="width:100%;height:55vh;border:none;border-radius:10px;background:#fff;"></iframe>';
    } else {
      content = '<div style="text-align:center;font-size:48px;padding:40px;">' + window._fileIcon(mime, att.file_name) + '</div><div style="text-align:center;font-size:12px;opacity:0.7;margin-top:8px;">Előnézet nem elérhető ehhez a formátumhoz — töltsd le a fájl megnyitásához.</div>';
    }
  }

  var modal = document.createElement('div');
  modal.style.cssText = 'background:var(--surface);border:1px solid var(--border);border-radius:16px;padding:20px;max-width:90vw;width:720px;max-height:90vh;overflow:auto;position:relative;';
  modal.onclick = function(e) { e.stopPropagation(); };

  modal.innerHTML =
    '<div style="display:flex;align-items:center;gap:10px;margin-bottom:14px;">' +
      '<span style="font-size:22px;">' + window._fileIcon(mime, att.file_name) + '</span>' +
      '<div style="flex:1;min-width:0;">' +
        '<div style="font-size:14px;font-weight:600;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">' + esc(att.file_name) + '</div>' +
        '<div style="font-size:11px;opacity:0.6;">' + esc(att.mime_type || '') + ' • ' + esc(window._formatFileSize(att.file_size)) + '</div>' +
      '</div>' +
      '<a href="' + esc(dlUrl) + '" download="' + esc(att.file_name) + '" style="background:var(--primary);color:#fff;text-decoration:none;padding:8px 16px;border-radius:8px;font-size:13px;font-weight:600;white-space:nowrap;">⬇️ Letöltés</a>' +
      '<button onclick="document.getElementById(\'filePreviewOverlay\').remove()" style="background:var(--surface2);color:var(--text);border:1px solid var(--border);padding:8px 12px;border-radius:8px;cursor:pointer;font-size:14px;">✕</button>' +
    '</div>' +
    content;

  overlay.appendChild(modal);
  document.body.appendChild(overlay);
  // ESC to close
  document.addEventListener('keydown', function _fpEsc(e) {
    if (e.key === 'Escape') {
      var o = document.getElementById('filePreviewOverlay');
      if (o) o.remove();
      document.removeEventListener('keydown', _fpEsc);
    }
  });
};


// ─────────────────────────────────────────────────────────
// ── Sidebar collapsible sections (Direktívák / Prioritás / Gyors Linkek) ──
window.toggleSidebarSection = function(key) {
  var body = document.getElementById(key + '-body');
  var header = document.querySelector('[data-collapse="' + key + '"]');
  if (!body || !header) return;
  var collapsed = !body.classList.contains('collapsed');
  body.classList.toggle('collapsed', collapsed);
  header.classList.toggle('open', !collapsed);
  try { localStorage.setItem('sidebar-collapsed-' + key, collapsed ? '1' : '0'); } catch (e) {}
};

(function restoreSidebarSections() {
  ['quicklinks', 'directives', 'priority'].forEach(function(key) {
    var body = document.getElementById(key + '-body');
    var header = document.querySelector('[data-collapse="' + key + '"]');
    if (!body || !header) return;
    var collapsed = false;
    try { collapsed = localStorage.getItem('sidebar-collapsed-' + key) === '1'; } catch (e) {}
    body.classList.toggle('collapsed', collapsed);
    header.classList.toggle('open', !collapsed);
  });
})();

// ─────────────────────────────────────────────────────────
// ── Chat command autocomplete (Telegram-style /commands) ──
// Format: cmd = command, args = argument format shown in palette AND inserted as
// a selected placeholder after the command (Telegram BotFather pattern).
window.CHAT_COMMANDS = [
  { cmd: '/help',    args: '',                desc: 'Elérhető parancsok listája' },
  { cmd: '/status',  args: '',                desc: 'Mesh és agent állapot riport' },
  { cmd: '/debate',  args: '<téma>',          desc: 'Vita indítása minden agent részvételével' },
  { cmd: '/ideas',   args: '<téma>',          desc: 'Ötletgyűjtés — agent-javaslatok [ÖTLET] jelölve → Ötletláda' },
  { cmd: '/all',     args: '<kérdés>',        desc: 'Közös elemzés — minden agent ugyanarra válaszol' },
  { cmd: '/ask',     args: '<agent> <kérdés>', desc: 'Célzott kérés egy agentnek (nova/morzsa/runa/tor)' },
  { cmd: '/clear',   args: '',                desc: 'Chat üzenetek törlése ebben a szobában' }
];

window.showCommandPalette = function(inputEl) {
  if (!inputEl) return;
  var val = inputEl.value;
  if (!val.startsWith('/')) { window.hideCommandPalette(); return; }
  var matches = window.CHAT_COMMANDS.filter(function(c) { return c.cmd.startsWith(val); });
  if (!matches.length || (val.indexOf(' ') >= 0 && !val.startsWith('/ask '))) { window.hideCommandPalette(); return; }

  // ── /ask agent-name second-level autocomplete ──
  if (val.startsWith('/ask ') && val.indexOf(' ') >= 0) {
    var askArg = val.slice(5).split(' ')[0].toLowerCase();
    var agents = ['nova', 'morzsa', 'runa', 'tor'].filter(function(a) { return a.startsWith(askArg); });
    if (agents.length) {
      window._renderPalette(inputEl, agents.map(function(a) {
        return { cmd: '/ask ' + a, args: '<kérdés>', desc: 'Kérdés a(z) ' + a + ' agentnek', _plain: true };
      }), val);
    } else {
      window.hideCommandPalette();
    }
    return;
  }
  window._renderPalette(inputEl, matches, val);
};

window._renderPalette = function(inputEl, matches, currentVal) {
  window.hideCommandPalette();
  var pal = document.createElement('div');
  pal.id = 'commandPalette';
  pal.style.cssText = 'position:absolute;bottom:100%;left:0;right:0;background:var(--surface);border:1px solid var(--border);border-radius:10px;box-shadow:0 6px 20px rgba(0,0,0,.4);z-index:10000;max-height:260px;overflow-y:auto;margin-bottom:4px;';
  window._paletteInput = inputEl;
  window._paletteItems = matches;
  window._paletteIndex = -1;

  matches.forEach(function(m, idx) {
    var row = document.createElement('div');
    row.className = 'palette-row';
    row.setAttribute('data-idx', idx);
    row.style.cssText = 'padding:9px 12px;cursor:pointer;font-size:13px;display:flex;gap:10px;align-items:center;';
    row.onmouseenter = function() { window._paletteHighlight(idx); };
    row.onclick = function() { window._palettePick(idx); };
    var argsHtml = m.args ? '<span style="color:var(--accent);font-family:monospace;font-size:12px;"> ' + esc(m.args) + '</span>' : '';
    row.innerHTML =
      '<span style="font-weight:600;color:var(--primary);font-family:monospace;">' + esc(m.cmd) + '</span>' +
      argsHtml +
      '<span style="color:var(--text3);font-size:11px;margin-left:auto;text-align:right;">' + esc(m.desc) + '</span>';
    pal.appendChild(row);
  });

  var wrap = inputEl.closest('#chatInputBar') || inputEl.closest('.input-area') || inputEl.parentElement;
  if (wrap && getComputedStyle(wrap).position === 'static') wrap.style.position = 'relative';
  (wrap || document.body).appendChild(pal);
};

window._paletteHighlight = function(idx) {
  var pal = document.getElementById('commandPalette');
  if (!pal) return;
  window._paletteIndex = idx;
  var rows = pal.querySelectorAll('.palette-row');
  rows.forEach(function(r, i) {
    r.style.background = (i === idx) ? 'var(--surface2)' : '';
  });
};

window._palettePick = function(idx) {
  var inputEl = window._paletteInput;
  var m = window._paletteItems && window._paletteItems[idx];
  if (!inputEl || !m) return;
  if (m._plain) {
    // Second-level (/ask <agent>) — keep question placeholder empty
    inputEl.value = m.cmd + ' ';
  } else if (m.args) {
    // Telegram pattern: insert command + argument placeholder, SELECT the placeholder
    // so the user can type over it immediately
    inputEl.value = m.cmd + ' ' + m.args + ' ';
    var selStart = (m.cmd + ' ').length;
    var selEnd = selStart + m.args.length;
    try { inputEl.setSelectionRange(selStart, selEnd); } catch (e) {}
  } else {
    inputEl.value = m.cmd + ' ';
  }
  inputEl.focus();
  window.hideCommandPalette();
};

window.hideCommandPalette = function() {
  var el = document.getElementById('commandPalette');
  if (el) el.remove();
  window._paletteIndex = -1;
};

// ── @mention autocomplete (agent picker from live /api/agents) ──
window.MENTION_AGENTS = [];

window.refreshMentionAgents = function() {
  var token = localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || '';
  fetch('/api/agents', { headers: { 'Authorization': 'Bearer ' + token } })
    .then(function(r) { return r.json(); })
    .then(function(d) {
      window.MENTION_AGENTS = (d.agents || []).map(function(a) {
        return { name: a.name, role: a.role || 'agent', status: a.status || 'online' };
      });
    })
    .catch(function() {});
};

window._mentionMatch = function(inputEl) {
  // Find the @token currently being typed (word containing the caret)
  var val = inputEl.value;
  var caret = inputEl.selectionStart != null ? inputEl.selectionStart : val.length;
  var m = /(^|\s)@(\w*)$/.exec(val.slice(0, caret));
  return m ? { token: m[2], start: caret - m[2].length - 1 } : null; // start = index of '@'
};

window.showMentionPalette = function(inputEl) {
  var m = window._mentionMatch(inputEl);
  if (!m) { window.hideMentionPalette(); return false; }
  if (!window.MENTION_AGENTS.length) window.refreshMentionAgents();
  var matches = window.MENTION_AGENTS.filter(function(a) {
    return a.name.toLowerCase().startsWith(m.token.toLowerCase());
  });
  if (!matches.length) { window.hideMentionPalette(); return false; }

  window.hideCommandPalette();
  window.hideMentionPalette();
  var pal = document.createElement('div');
  pal.id = 'mentionPalette';
  pal.style.cssText = 'position:absolute;bottom:100%;left:0;right:0;background:var(--surface);border:1px solid var(--border);border-radius:10px;box-shadow:0 6px 20px rgba(0,0,0,.4);z-index:10000;max-height:240px;overflow-y:auto;margin-bottom:4px;';
  window._mentionInput = inputEl;
  window._mentionItems = matches;
  window._mentionIndex = -1;
  window._mentionTokenStart = m.start;

  matches.forEach(function(a, idx) {
    var row = document.createElement('div');
    row.className = 'mention-row';
    row.setAttribute('data-idx', idx);
    row.style.cssText = 'padding:9px 12px;cursor:pointer;font-size:13px;display:flex;gap:10px;align-items:center;';
    row.onmouseenter = function() { window._mentionHighlight(idx); };
    row.onclick = function() { window._mentionPick(idx); };
    var dot = a.status === 'online' || a.status === 'connected' ? '🟢' : '🔴';
    row.innerHTML =
      '<span>' + dot + '</span>' +
      '<span style="font-weight:600;color:var(--primary);">@' + esc(a.name) + '</span>' +
      '<span style="color:var(--text3);font-size:11px;margin-left:auto;">' + esc(a.role || '') + '</span>';
    pal.appendChild(row);
  });

  var wrap = inputEl.closest('#chatInputBar') || inputEl.closest('.input-area') || inputEl.parentElement;
  if (wrap && getComputedStyle(wrap).position === 'static') wrap.style.position = 'relative';
  (wrap || document.body).appendChild(pal);
  return true;
};

window._mentionHighlight = function(idx) {
  var pal = document.getElementById('mentionPalette');
  if (!pal) return;
  window._mentionIndex = idx;
  var rows = pal.querySelectorAll('.mention-row');
  rows.forEach(function(r, i) { r.style.background = (i === idx) ? 'var(--surface2)' : ''; });
};

window._mentionPick = function(idx) {
  var inputEl = window._mentionInput;
  var a = window._mentionItems && window._mentionItems[idx];
  if (!inputEl || !a) return;
  var val = inputEl.value;
  var start = window._mentionTokenStart;
  // Replace the @token with the full agent name
  inputEl.value = val.slice(0, start) + '@' + a.name + ' ' + val.slice(inputEl.selectionStart != null ? inputEl.selectionStart : val.length);
  var newCaret = start + a.name.length + 2;
  try { inputEl.setSelectionRange(newCaret, newCaret); } catch (e) {}
  inputEl.focus();
  window.hideMentionPalette();
};

window.hideMentionPalette = function() {
  var el = document.getElementById('mentionPalette');
  if (el) el.remove();
  window._mentionIndex = -1;
};

window.attachCommandAutocomplete = function(inputEl) {
  if (!inputEl || inputEl._cmdAttached) return;
  inputEl._cmdAttached = true;
  inputEl.addEventListener('input', function() {
    var isMention = window.showMentionPalette(inputEl);
    if (!isMention) window.showCommandPalette(inputEl);
  });
  inputEl.addEventListener('blur', function() {
    setTimeout(window.hideCommandPalette, 250);
    setTimeout(window.hideMentionPalette, 250);
  });
  // Telegram-style keyboard navigation: ↑/↓ select, Tab/Enter complete, Esc close
  inputEl.addEventListener('keydown', function(e) {
    var pal = document.getElementById('commandPalette') || document.getElementById('mentionPalette');
    if (!pal) return;
    var items = pal.id === 'commandPalette' ? window._paletteItems : window._mentionItems;
    if (!items || !items.length) return;
    var n = items.length;
    var isCmd = pal.id === 'commandPalette';
    var curIdx = isCmd ? window._paletteIndex : window._mentionIndex;
    var highlight = isCmd ? window._paletteHighlight : window._mentionHighlight;
    var pick = isCmd ? window._palettePick : window._mentionPick;
    if (e.key === 'ArrowDown') {
      e.preventDefault();
      highlight(((curIdx != null ? curIdx : -1) + 1) % n);
    } else if (e.key === 'ArrowUp') {
      e.preventDefault();
      highlight(((curIdx != null ? curIdx : 0) - 1 + n) % n);
    } else if (e.key === 'Tab' || e.key === 'Enter') {
      if (isCmd || curIdx >= 0 || n === 1) {
        e.preventDefault();
        pick(curIdx >= 0 ? curIdx : 0);
      }
    } else if (e.key === 'Escape') {
      window.hideCommandPalette();
      window.hideMentionPalette();
    }
  });
};

// ─────────────────────────────────────────────────────────
function scrollToBottom() {
  var el = document.getElementById("messages");
  el.scrollTop = el.scrollHeight;
}

function incrementMsgCount() {
  var el = document.getElementById("msgCount");
  if (el) el.textContent = parseInt(el.textContent || "0") + 1;
}

function deleteMessage(msgId) {
  if (!msgId || !confirm("Törlöd ezt az üzenetet?")) return;
  fetch("/api/messages/" + encodeURIComponent(msgId), {
    method: "DELETE",
    headers: { "Authorization": "Bearer " + (localStorage.getItem("mesh_token") || "") }
  }).then(function(r) { return r.json(); }).then(function(d) {
    if (d.status === "deleted") {
      var el = document.getElementById("msg-" + msgId);
      if (el) el.remove();
      // Remove from local history
      messageHistory = messageHistory.filter(function(m) { return m.id !== msgId; });
      for (var ch in channelMessages) {
        channelMessages[ch] = channelMessages[ch].filter(function(m) { return m.id !== msgId; });
      }
    }
  }).catch(function() {});
}

function escapeHtml(t) { var d = document.createElement("div"); d.textContent = t; return d.innerHTML; }
function esc(s) { return String(s == null ? '' : s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;').replace(/'/g,'&#39;'); }

function log(msg) {
  var el = document.getElementById("sysLog");
  var time = new Date().toLocaleTimeString("hu-HU");
  el.innerHTML = '<div>[' + time + '] ' + msg + '</div>' + el.innerHTML;
}

// ─── Auth ──────────────────────────────────────────────
function showAuth() {
  document.getElementById("authModal").style.display = "flex";
  document.getElementById("authError").textContent = "";
  var saved = localStorage.getItem("a2a_username");
  if (saved) document.getElementById("authUsername").value = saved;
  document.getElementById("authUsername").focus();
}

function switchAuthMode() {
  var isLogin = document.getElementById("authTitle").textContent.indexOf("Bejelentkez") >= 0;
  document.getElementById("authTitle").textContent = isLogin ? "🤖 Regisztráció" : "🤖 Bejelentkezés";
  document.getElementById("authSubtitle").textContent = isLogin ? "Új felhasználó létrehozása (csak owner)" : "Add meg a felhasználóneved és jelszavad";
  document.getElementById("authSubmitBtn").textContent = isLogin ? "Regisztráció" : "Bejelentkezés";
  document.getElementById("authExtraFields").style.display = isLogin ? "block" : "none";
  document.getElementById("authSwitch").innerHTML = isLogin
    ? "Már van fiókod? <a onclick=\"switchAuthMode()\">Jelentkezz be</a>"
    : "Még nincs fiókod? <a onclick=\"switchAuthMode()\">Regisztrálj</a>";
  document.getElementById("authError").textContent = "";
}

function submitAuth() {
  var username = document.getElementById("authUsername").value.trim().toLowerCase();
  var password = document.getElementById("authPassword").value;
  var errEl = document.getElementById("authError");
  errEl.textContent = "";
  if (!username || !password) { errEl.textContent = "Felhasználónév és jelszó kötelező"; return; }
  var isLogin = document.getElementById("authTitle").textContent.indexOf("Regisztráció") < 0;
  if (isLogin) {
    fetch("/api/auth/login", {
      method: "POST", headers: {"Content-Type": "application/json"},
      body: JSON.stringify({username: username, password: password})
    }).then(function(r) { return r.json(); }).then(function(d) {
      if (d.error) { errEl.textContent = d.error; } else {
        authToken = d.token; authUser = d.user;
        window._chatUsername = d.user.username;
        localStorage.setItem("a2a_token", d.token);
        localStorage.setItem("mesh_token", d.token);
        localStorage.setItem("a2a_username", d.user.username);
        document.getElementById("authModal").style.display = "none";
        document.getElementById("userBadge").style.display = "flex";
        updateUserBadge();
        initWebSocket();
        loadStatus();
        loadMessages();
        loadAgents();
        checkAdminPanel();
        // Auto-refresh messages every 10 seconds (Telegram-style live updates)
        if (window._msgRefreshInterval) clearInterval(window._msgRefreshInterval);
        window._msgRefreshInterval = setInterval(function() {
          loadMessages();
          loadAgents();
        }, 3000);
      }
    }).catch(function() { errEl.textContent = "Hálózati hiba"; });
  } else {
    var displayName = document.getElementById("authDisplayName").value.trim() || username;
    var role = document.getElementById("authRole").value;
    fetch("/api/auth/register", {
      method: "POST", headers: {"Content-Type": "application/json", "Authorization": "Bearer " + authToken},
      body: JSON.stringify({username: username, display_name: displayName, password: password, role: role})
    }).then(function(r) { return r.json(); }).then(function(d) {
      if (d.error) { errEl.textContent = d.error; } else {
        errEl.style.color = "var(--success)";
        errEl.textContent = "Felhasználó '" + d.user.username + "' sikeresen regisztrálva!";
        setTimeout(function() { errEl.style.color = ""; switchAuthMode(); }, 2000);
      }
    }).catch(function() { errEl.textContent = "Hálózati hiba"; });
  }
}

function logout() {
  var token = authToken || localStorage.getItem("a2a_token");
  if (token) { fetch("/api/auth/logout", {method: "POST", headers: {"Authorization": "Bearer " + token}}).catch(function() {}); }
  authToken = null; authUser = null;
  localStorage.removeItem("a2a_token");
  localStorage.removeItem("mesh_token");
  if (ws) ws.close();
  showAuth();
}

function updateUserBadge() {
  var badge = document.getElementById("userBadge");
  if (authUser) {
    var rb = authUser.role === "owner" ? '<span class="role-badge owner">owner</span>' : '<span class="role-badge user">user</span>';
    badge.innerHTML = authUser.display_name + " " + rb + " ▾";
  }
}

// ─── WebSocket ──────────────────────────────────────────
function initWebSocket() {
  var proto = location.protocol === "https:" ? "wss:" : "ws:";
  var wsUrl = proto + "//" + location.host + "/ws";
  if (authToken) wsUrl += "?token=" + encodeURIComponent(authToken);
  ws = new WebSocket(wsUrl);
  ws.onopen = function() { log("WebSocket connected"); loadStatus(); loadMessages(); loadAgents(); };
  ws.onmessage = function(e) {
    var data = JSON.parse(e.data);
    switch(data.type) {
      case "connected": nodeId = data.node; document.getElementById("nodeName").textContent = data.node; break;
      case "status": updateStatus(data.data); break;
      case "new_message":
        // A real message from an agent replaces their typing bubble (Telegram pattern)
        try { if (data.message && data.message.sender) window.hideTypingIndicator(data.message.sender); } catch(e) {}
        // Force-add to general channel for unified view
        if (!channelMessages["general"]) channelMessages["general"] = [];
        channelMessages["general"].push(data.message);
        if (currentChannel === "general") { renderChannelMessages(); scrollMessages(); }
        // Also add to DM channel for DM view
        addMessage(data.message);
        incrementMsgCount();
        var _ch = currentChannel || "general";
        if (_ch !== "general" && typeof window._loadChatMessages === "function") {
          window._loadChatMessages(_ch, true);
        }
        break;
      case "agent_typing":
        // Telegram-style: agent started thinking — show typing bubble
        try { window.showTypingIndicator(data.agent, data.chat_type); } catch(e) {}
        break;
      case "agent_typing_stop":
        // Agent finished — remove typing bubble
        try { window.hideTypingIndicator(data.agent); } catch(e) {}
        break;
      case "file_transfer":
        // Show file transfer notification in chat
        var ftMsg = {
          id: "ft_" + Date.now(),
          sender: data.sender || "system",
          recipient: data.recipient || "broadcast",
          type: "system",
          content: "📎 Fájl megosztva: " + (data.filename || "?") + " (" + (data.results ? data.results.length : 0) + " peer)",
          priority: 5,
          timestamp: new Date().toISOString(),
          source: "web_dashboard"
        };
        addMessage(ftMsg);
        break;
      case "message_deleted":
        var el = document.getElementById("msg-" + data.message_id);
        if (el) el.remove();
        messageHistory = messageHistory.filter(function(m) { return m.id !== data.message_id; });
        break;
      case "error": log("Hiba: " + data.message); break;
    }
  };
  ws.onclose = function() { log("WebSocket disconnected, reconnecting..."); setTimeout(initWebSocket, 3000); };
  ws.onerror = function() {};
}

// ─── Data Loading ──────────────────────────────────────
function loadStatus() {
  fetch("/api/status").then(function(r) { return r.json(); }).then(function(d) { updateStatus(d); }).catch(function() {});
  loadTaskQueue();
}

function loadMessages() {
  // Main chat ONLY handles general/broadcast channel.
  // DM channels are handled by _loadChatMessages() with /api/chat/messages?with=X
  var ch = currentChannel || "general";
  if (ch !== "general") {
    // DM channel: use the DM panel loader instead
    if (typeof window._loadChatMessages === "function") {
      window._loadChatMessages(ch, true);
    }
    return;
  }
  var token = localStorage.getItem("a2a_token") || localStorage.getItem("mesh_token") || "";
  fetch("/api/chat/messages?limit=30", {
  }).then(function(r) {
    if (r.status === 401) { console.warn("[DM] 401 — token expired"); return null; }
    if (r.status === 429) { console.warn("[DM] 429 — rate limited"); return null; }
    return r.json();
  }).then(function(d) {
    if (!d) return; // 401/429 — keep existing messages, don't wipe
    // Store pagination state
    window._chatHasMore = d.has_more || false;
    window._chatOldestId = d.oldest_id || null;
    // Filter messages based on current channel
    messageHistory = [];
    if (!channelMessages[currentChannel || "general"]) {
      channelMessages[currentChannel || "general"] = [];
    }
    var ch = currentChannel || "general";
    var username = (authUser ? authUser.username : localStorage.getItem("a2a_username")) || "zsolt";
    // Clear current channel messages before re-adding from server (avoid duplicates)
    channelMessages[currentChannel || "general"] = [];
    // API returns DESC (newest first) — reverse to chronological (oldest first)
    // so renderChannelMessages appends newest at the BOTTOM, matching the DM loader.
    var sortedMsgs = (d.messages || []).slice().reverse();
    sortedMsgs.forEach(function(m) {
      m.content = m.content || m.text || "";
      m.timestamp = m.timestamp || m.created_at || "";
      // Normalize field names: API returns msg_type, JS expects type
      m.type = m.type || m.msg_type || "";
      // Normalize sender/recipient for addMessage routing
      if (ch === "general") {
        // General channel: ONLY show broadcast messages and agent replies to broadcast
        // DM messages go to their own DM channels (not general)
        var mType = m.msg_type || m.type || "";
        if (mType === "ack" || mType === "heartbeat" || mType === "skills_announcement" || mType === "diagnostic_report") return;
        // Skip DM messages (recipient is a specific agent, not broadcast)
        if (m.recipient && m.recipient !== "broadcast" && m.recipient !== username && mType !== "agent_reply") return;
        // Skip agent_reply DMs (sender is a specific agent, recipient is username — not broadcast)
        if (mType === "agent_reply" && m.recipient && m.recipient !== "broadcast") return;
      } else {
        // DM channel: only show messages between user and this agent
        var isMine = (m.sender === username && m.recipient === ch);
        var isTheirs = (m.sender === ch && m.recipient === username);
        if (!isMine && !isTheirs) return;
      }
      // For general channel: force-add to general channel regardless of DM detection
      if (ch === "general") {
        if (!channelMessages["general"]) channelMessages["general"] = [];
        // Dedup by id
        var exists = false;
        for (var k = 0; k < channelMessages["general"].length; k++) {
          if (channelMessages["general"][k].id === m.id) { exists = true; break; }
        }
        if (!exists) channelMessages["general"].push(m);
      } else {
        addMessage(m, false);
      }
    });
    document.getElementById("msgCount").textContent = d.total_count || d.total || (d.messages || []).length;
    renderChannelMessages();
    scrollToBottom();
    // Init scroll listener for lazy loading
    _initChatScrollListener();
  }).catch(function(e) {
    console.warn("[DM] loadMessages error:", e);
    // Keep existing messages on error — don't fallback to /api/messages
  });
}

function loadAgents() {
  var token = localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || '';
  fetch("/api/agents", { headers: { 'Authorization': 'Bearer ' + token } })
    .then(function(r) { return r.json(); }).then(function(d) {
    renderAgents(d.agents);
    document.getElementById("totalAgents").textContent = d.total;
    var sel = document.getElementById("recipientSelect");
    // Preserve current selection
    var currentVal = sel.value;
    sel.innerHTML = '<option value="">📢 Mindenkinek</option>';
    d.agents.forEach(function(a) {
      // Include self node (nova) so user can DM their own agent
      var opt = document.createElement("option");
      opt.value = a.name;
      var label = a.name === nodeId ? "👤 " + a.name + " (saját)" : "👤 " + a.name + " (" + a.role + ")";
      opt.textContent = label;
      sel.appendChild(opt);
      // Also add DM channel with status
      addDMChannel(a.name, a.status);
    });
    // Restore selection
    sel.value = currentVal;
  }).catch(function() {});
}

function renderAgents(agents) {
  // Update DM channels with status indicators
  agents.forEach(function(a) { addDMChannel(a.name, a.status); });
  // Also render agent status cards in the agent list area
  var agentList = document.getElementById("agentListCards");
  if (!agentList) {
    // Create the agent list container if it doesn't exist
    var section = document.getElementById("dmList");
    if (section) {
      // Render agent status directly in the DM list
      var existingCards = document.getElementById("dmAgentStatusCards");
      if (!existingCards) {
        existingCards = document.createElement("div");
        existingCards.id = "dmAgentStatusCards";
        existingCards.style.cssText = "margin-top:4px;padding:0;";
        section.parentNode.insertBefore(existingCards, section.nextSibling);
      }
      agentList = existingCards;
    }
  }
  if (!agentList) return;

  // Build status cards for each agent (excluding self)
  var html = "";
  agents.forEach(function(a) {
    var statusIcon = (a.status === "online") ? "🟢" : (a.status === "available" || a.status === "connected") ? "🟡" : "🔴";
    var statusText = (a.status === "online") ? "Online" : (a.status === "available") ? "Elérhető" : (a.status === "connected") ? "Csatlakozott" : "Inaktív";
    var transports = a.transports || {};
    var transportParts = [];
    if (transports.p2p) transportParts.push("P2P✓");
    if (transports.pg || transports.pg_notify) transportParts.push("PG✓");
    if (transports.http) transportParts.push("HTTP✓");
    var transportStr = transportParts.length > 0 ? transportParts.join(" ") : "—";
    html += '<div style="padding:3px 0;border-bottom:1px solid var(--border,rgba(255,255,255,.06));">' +
      '<div style="display:flex;align-items:center;justify-content:space-between;font-size:11px;">' +
      '<span>' + statusIcon + ' <strong>' + escapeHtml(a.name) + '</strong></span>' +
      '<span style="color:var(--text3);font-size:10px;">' + transportStr + '</span>' +
    '</div>';
    // Show skills as small tags
    var skills = a.skills || [];
    if (skills.length > 0) {
      var skillTags = skills.slice(0, 5).map(function(s) {
        var short = s.split('_').pop();
        return '<span style="background:rgba(79,140,255,.15);color:var(--primary);padding:1px 4px;border-radius:3px;font-size:9px;margin:1px;">' + escapeHtml(short) + '</span>';
      }).join('');
      if (skills.length > 5) skillTags += '<span style="color:var(--text3);font-size:9px;">+' + (skills.length - 5) + '</span>';
      html += '<div style="display:flex;flex-wrap:wrap;gap:1px;margin-top:1px;">' + skillTags + '</div>';
    }
    // Show capabilities as green badges
    var caps = a.capabilities || [];
    if (caps.length > 0) {
      var capTags = caps.slice(0, 8).map(function(c) {
        return '<span style="background:rgba(76,175,80,.12);color:#4caf50;padding:1px 4px;border-radius:3px;font-size:8px;margin:1px;">' + escapeHtml(c) + '</span>';
      }).join('');
      if (caps.length > 8) capTags += '<span style="color:var(--text3);font-size:8px;">+' + (caps.length - 8) + '</span>';
      html += '<div style="display:flex;flex-wrap:wrap;gap:1px;margin-top:1px;\">' + capTags + '</div>';
    }
    html += '</div>';
  });
  agentList.innerHTML = html;
}

function updateStatus(data) {
  document.getElementById("msgCount").textContent = data.messages_sent || 0;
  document.getElementById("localStore").textContent = (data.local_store || {}).outbound_pending || 0;
  document.getElementById("p2pPeers").textContent = (data.peer_discovery || {}).connected_peers || 0;
  document.getElementById("userCount").textContent = ((data.dashboard || {}).connected_users || 0) + " user";
  var tr = data.transports || {};
  var inner = tr.transports || tr;
  document.getElementById("transportGrid").innerHTML =
    statBox("PG", inner.pg_notify !== undefined ? inner.pg_notify : inner.pg) +
    statBox("P2P", inner.p2p) + statBox("HTTP", inner.http) + statBox("BLE", inner.ble);
  var as = data.auto_steer || {};
  document.getElementById("steerGrid").innerHTML =
    statBox("Interrupts", as.interrupts || 0) + statBox("Queued", as.queued || 0) +
    statBox("Processed", as.processed || 0) + statBox("Backlog", as.backlog || 0);
}

// ─── Task Queue Widget ──────────────────────────────
function loadTaskQueue() {
  if (!authToken) return;
  // Fetch stats + available tasks in parallel
  Promise.all([
    fetch("/api/delegations/stats", {headers: {"Authorization": "Bearer " + authToken}}).then(function(r){return r.json()}),
    fetch("/api/delegations?status=available&limit=10", {headers: {"Authorization": "Bearer " + authToken}}).then(function(r){return r.json()}),
    fetch("/api/delegations?status=running&limit=10", {headers: {"Authorization": "Bearer " + authToken}}).then(function(r){return r.json()})
  ]).then(function(results) {
    var stats = results[0] || {};
    var available = (results[1] || {}).delegations || [];
    var running = (results[2] || {}).delegations || [];

    // Stats badges
    var badges = [];
    if (stats.available > 0) badges.push('<span style="background:var(--info);color:#fff;padding:2px 8px;border-radius:10px;font-size:10px">' + stats.available + ' available</span>');
    if (stats.running > 0) badges.push('<span style="background:var(--success);color:#fff;padding:2px 8px;border-radius:10px;font-size:10px">' + stats.running + ' running</span>');
    if (stats.completed > 0) badges.push('<span style="background:var(--surface2);color:var(--text2);padding:2px 8px;border-radius:10px;font-size:10px">' + stats.completed + ' done</span>');
    document.getElementById("taskQueueStats").innerHTML = badges.length ? badges.join("") : '<span style="color:var(--text3);font-size:11px">Nincs aktív task</span>';

    // Task list — running first, then available
    var items = [];
    function taskItem(t, isRunning) {
      var icon = isRunning ? "🔄" : "⏳";
      var color = isRunning ? "var(--success)" : "var(--info)";
      var subj = escapeHtml(t.subject || t.task_type || "?");
      if (subj.length > 35) subj = subj.substring(0, 35) + "…";
      var agent = escapeHtml(t.assigned_agent || t.to_agent || "any");
      var age = t.created_at ? timeAgo(t.created_at) : "";
      return '<div style="padding:4px 0;border-bottom:1px solid var(--border)">' +
        '<span style="color:' + color + '">' + icon + '</span> ' +
        '<span style="font-weight:500">' + subj + '</span> ' +
        '<span style="color:var(--text3)">@' + agent + '</span>' +
        (age ? '<span style="color:var(--text3);float:right">' + age + '</span>' : '') +
      '</div>';
    }
    running.forEach(function(t) { items.push(taskItem(t, true)); });
    available.forEach(function(t) { items.push(taskItem(t, false)); });
    document.getElementById("taskQueueList").innerHTML = items.length ? items.join("") : "";
  }).catch(function() {});
}

function timeAgo(iso) {
  if (!iso) return "";
  var d = new Date(iso);
  var s = Math.floor((Date.now() - d.getTime()) / 1000);
  if (s < 60) return s + "s";
  if (s < 3600) return Math.floor(s/60) + "m";
  return Math.floor(s/3600) + "h";
}

function statBox(label, val) {
  if (typeof val === "object" && val !== null) {
    var avail = val.available !== undefined ? val.available : false;
    return '<div class="stat-box"><div class="label">' + label + '</div><div class="value ' + (avail ? "green" : "amber") + '">' + (avail ? "✓" : "✗") + '</div></div>';
  }
  if (typeof val === "boolean") {
    return '<div class="stat-box"><div class="label">' + label + '</div><div class="value ' + (val ? "green" : "amber") + '">' + (val ? "✓" : "✗") + '</div></div>';
  }
  return '<div class="stat-box"><div class="label">' + label + '</div><div class="value">' + val + '</div></div>';
}

// ─── Admin: Node Approval ──────────────────────────────
function loadPendingNodes() {
  if (!authToken || !authUser || authUser.role !== "owner") return;
  fetch("/api/nodes/pending", {headers: {"Authorization": "Bearer " + authToken}})
    .then(function(r) { return r.json(); }).then(function(d) {
      var el = document.getElementById("pendingNodes");
      if (!d.nodes || d.nodes.length === 0) {
        el.innerHTML = '<div style="color:var(--text3);font-size:12px;text-align:center;padding:8px">Nincs jóváhagyásra váró node</div>';
        return;
      }
      el.innerHTML = d.nodes.map(function(n) {
        return '<div style="background:var(--surface2);border:1px solid var(--border);border-radius:6px;padding:10px;margin-bottom:8px;">' +
          '<div style="font-weight:600;font-size:13px;">🆕 ' + escapeHtml(n.node_name) + '</div>' +
          '<div style="font-size:11px;color:var(--text3);margin-top:2px;">' +
            'Szerep: ' + escapeHtml(n.role || "—") + ' · IP: ' + escapeHtml(n.host || "?") + ':' + (n.p2p_port || "?") +
          '</div>' +
          '<div style="margin-top:8px;display:flex;gap:6px;">' +
            '<button class="btn btn-sm" style="background:var(--success)" onclick="approveNode(\'' + escapeHtml(n.node_name) + '\')">✅ Jóváhagy</button>' +
            '<button class="btn btn-sm btn-danger" onclick="rejectNode(\'' + escapeHtml(n.node_name) + '\')">❌ Elutasít</button>' +
          '</div></div>';
      }).join('');
    }).catch(function() {});
}

function approveNode(nodeName) {
  fetch("/api/nodes/" + encodeURIComponent(nodeName) + "/approve", {method: "POST", headers: {"Authorization": "Bearer " + authToken}})
    .then(function(r) { return r.json(); }).then(function(d) {
      if (d.status === "approved") { log("✅ Node '" + nodeName + "' jóváhagyva"); loadPendingNodes(); loadAllNodes(); loadAgents(); }
      else { log("❌ Hiba: " + (d.error || "ismeretlen")); }
    }).catch(function() { log("❌ Hálózati hiba"); });
}

function rejectNode(nodeName) {
  fetch("/api/nodes/" + encodeURIComponent(nodeName) + "/reject", {method: "POST", headers: {"Authorization": "Bearer " + authToken}})
    .then(function(r) { return r.json(); }).then(function(d) {
      if (d.status === "rejected") { log("🚫 Node '" + nodeName + "' elutasítva"); loadPendingNodes(); loadAllNodes(); }
      else { log("❌ Hiba: " + (d.error || "ismeretlen")); }
    }).catch(function() { log("❌ Hálózati hiba"); });
}

function loadAllNodes() {
  if (!authToken) return;
  fetch("/api/nodes", {headers: {"Authorization": "Bearer " + authToken}})
    .then(function(r) { return r.json(); }).then(function(d) {
      if (!d.nodes || d.nodes.length === 0) return;
      var el = document.getElementById("allNodesList");
      isAdmin = authUser && authUser.role === "owner";
      document.getElementById("adminSection").style.display = isAdmin ? "block" : "none";
      el.innerHTML = d.nodes.map(function(n) {
        var statusIcon = (n.status === "online" || n.status === "connected" || n.status === "active") ? "🟢" : n.status === "available" ? "🟡" : n.status === "pending" ? "🟡" : "🔴";
        var statusText = n.status === "online" ? "Online" : n.status === "connected" ? "Csatlakozott" : n.status === "active" ? "Aktív" : n.status === "available" ? "Elérhető" : n.status === "pending" ? "Függő" : "Inaktív";
        var transports = [];
        if (n.pg_available) transports.push({name:"PG", on: !!n.pg_available});
        if (n.p2p_available) transports.push({name:"P2P", on: !!n.p2p_available});
        if (n.http_available) transports.push({name:"HTTP", on: !!n.http_available});
        if (n.ble_available) transports.push({name:"BLE", on: !!n.ble_available});
        var transportHtml = transports.map(function(t) {
          return '<span class="transport-tag' + (t.on ? '' : ' offline') + '">' + t.name + '</span>';
        }).join('');
        // Build detail section data
        var detailId = "nodeDetail_" + escapeHtml(n.node_name).replace(/[^a-zA-Z0-9]/g, "_");
        var uptimeVal = n.uptime_seconds != null ? formatUptime(n.uptime_seconds) : "—";
        var lastSeenVal = n.last_seen ? formatTimeAgo(n.last_seen) : "—";
        var healthScore = n.health_score != null ? n.health_score : null;
        var msgCount = n.message_count != null ? n.message_count : (n.messages_sent != null ? n.messages_sent : "—");
        var version = n.version || "—";
        var skills = n.skills || [];
        var caps = n.capabilities || [];
        var skillsHtml = "";
        if (skills.length > 0 || caps.length > 0) {
          var skillsPart = skills.map(function(s) { return '<span class="skill-tag">' + escapeHtml(s) + '</span>'; }).join('');
          var capsPart = caps.map(function(c) { return '<span class="skill-tag" style="color:#4caf50;background:rgba(76,175,80,.12)">' + escapeHtml(c) + '</span>'; }).join('');
          skillsHtml = '<div class="node-detail-item" style="grid-column:span 2"><div class="dlabel">Képességek (' + caps.length + ') / Skillek (' + skills.length + ')</div><div class="node-detail-skills">' +
            capsPart + skillsPart +
          '</div></div>';
        }
        var healthHtml = "";
        if (healthScore !== null) {
          var pct = Math.round(healthScore * 100);
          var hColor = pct >= 80 ? "green" : pct >= 50 ? "amber" : "red";
          healthHtml = '<div class="node-detail-item"><div class="dlabel">Egészség (Health)</div><div class="dvalue ' + hColor + '">' + pct + '%</div><div class="health-bar"><div class="health-fill" style="width:' + pct + '%;background:var(--' + hColor + ')"></div></div></div>';
        }
        var p2pStatus = n.p2p_available ? '<span class="dvalue green">✓ Aktív</span>' : '<span class="dvalue red">✗ Inaktív</span>';
        return '<div class="node-card">' +
          '<div class="node-card-header">' +
            '<div class="node-name">' + statusIcon + ' ' + escapeHtml(n.node_name) + '</div>' +
            '<div class="node-meta">' +
              '<span style="color:var(--text3)">(' + escapeHtml(n.role || "—") + ')</span>' +
              '<span class="node-transports">' + transportHtml + '</span>' +
              '<button class="node-detail-toggle" onclick="toggleNodeDetail(\'' + detailId + '\', this)">Részletek</button>' +
            '</div>' +
          '</div>' +
          '<div class="node-detail-content" id="' + detailId + '">' +
            '<div class="node-detail-grid">' +
              '<div class="node-detail-item"><div class="dlabel">Verzió</div><div class="dvalue blue">' + escapeHtml(version) + '</div></div>' +
              '<div class="node-detail-item"><div class="dlabel">Szerep (Role)</div><div class="dvalue">' + escapeHtml(n.role || "—") + '</div></div>' +
              '<div class="node-detail-item"><div class="dlabel">Futásidő (Uptime)</div><div class="dvalue">' + uptimeVal + '</div></div>' +
              '<div class="node-detail-item"><div class="dlabel">P2P Státusz</div>' + p2pStatus + '</div>' +
              '<div class="node-detail-item"><div class="dlabel">Utoljára aktív</div><div class="dvalue">' + lastSeenVal + '</div></div>' +
              '<div class="node-detail-item"><div class="dlabel">Üzenetek száma</div><div class="dvalue blue">' + msgCount + '</div></div>' +
              healthHtml +
              skillsHtml +
              '<div class="node-detail-item" style="grid-column:span 2"><div class="dlabel">Cím</div><div class="dvalue" style="font-size:11px;font-weight:400;color:var(--text3)">' + escapeHtml(n.host || "?") + ':' + (n.p2p_port || "?") + '</div></div>' +
            '</div>' +
          '</div>' +
        '</div>';
      }).join('');
    }).catch(function() {});
}

function toggleNodeDetail(detailId, btn) {
  var el = document.getElementById(detailId);
  if (!el) return;
  if (el.classList.contains("visible")) {
    el.classList.remove("visible");
    btn.classList.remove("open");
    btn.textContent = "Részletek";
  } else {
    el.classList.add("visible");
    btn.classList.add("open");
    btn.textContent = "Rejtés";
  }
}

function formatUptime(seconds) {
  if (seconds < 60) return seconds + " mp";
  if (seconds < 3600) return Math.floor(seconds / 60) + " p " + (seconds % 60) + " mp";
  if (seconds < 86400) return Math.floor(seconds / 3600) + " ó " + Math.floor((seconds % 3600) / 60) + " p";
  return Math.floor(seconds / 86400) + " nap " + Math.floor((seconds % 86400) / 3600) + " ó";
}

function formatTimeAgo(ts) {
  if (!ts) return "—";
  var date = new Date(ts);
  var now = new Date();
  var diff = Math.floor((now - date) / 1000);
  if (diff < 60) return "most";
  if (diff < 3600) return Math.floor(diff / 60) + " perce";
  if (diff < 86400) return Math.floor(diff / 3600) + " órája";
  return Math.floor(diff / 86400) + " napja";
}

function checkAdminPanel() {
  isAdmin = authUser && authUser.role === "owner";
  document.getElementById("adminSection").style.display = isAdmin ? "block" : "none";
  if (isAdmin) loadPendingNodes();
  loadAllNodes();
  loadQuickLinks();
}

// ─── Quick Links — dynamic node dashboard links ──────────
function loadQuickLinks() {
  fetch("/api/registry", {headers: {"Authorization": "Bearer " + (localStorage.getItem("mesh_token") || "")}})
    .then(function(r) { return r.json(); })
    .then(function(d) {
      var el = document.getElementById("quickLinksList");
      if (!el) return;
      var agents = d.agents || {};
      var html = "";

      // Topology view — this node
      html += '<div class="channel-item" onclick="window.open(\'/topology\',\'_blank\')">';
      html += '<span class="icon">🌐</span> Topológia Nézet';
      html += '</div>';

      // Dashboard links for each connected agent
      var selfName = (d.self_name || nodeId || "").toLowerCase();
      var selfHost = location.hostname;
      var selfPort = location.port || (location.protocol === "https:" ? "443" : "80");
      // If accessing via proxy (non-8650 port), use proxy URL for self, direct for others
      var useProxy = (selfPort !== "8650" && selfPort !== "8645");

      Object.keys(agents).forEach(function(name) {
        var info = agents[name];
        var caps = info.capabilities || [];
        var hasDash = caps.some(function(c) { return c.toLowerCase() === "dashboard"; });
        // All mesh nodes have dashboards — show regardless of capability
        var isSelf = (name.toLowerCase() === selfName);
        var dashUrl;

        if (isSelf) {
          // Self node — link to current dashboard (proxy or direct)
          dashUrl = location.protocol + "//" + selfHost + ":" + selfPort + "/dashboard";
        } else {
          // Peer node — use direct health_port from mesh data
          var peerHost = info.host || info.endpoint || "";
          if (peerHost.indexOf("://") >= 0) { peerHost = peerHost.split("://")[1]; }
          if (peerHost.indexOf(":") >= 0) { peerHost = peerHost.split(":")[0]; }
          var peerPort = info.health_port || 8650;
          dashUrl = location.protocol + "//" + peerHost + ":" + peerPort + "/dashboard";
        }

        var label = name.charAt(0).toUpperCase() + name.slice(1) + " Dashboard";
        var icon = isSelf ? "🏠" : "📊";

        html += '<div class="channel-item" onclick="window.open(\'' + dashUrl + '\',\'_blank\')">';
        html += '<span class="icon">' + icon + '</span> ' + label;
        html += '</div>';
      });

      // If no agents have dashboard capability, show a placeholder
      if (html.indexOf("Dashboard") === -1 && Object.keys(agents).length > 0) {
        html += '<div style="font-size:12px;color:var(--text3);padding:8px 16px;">Nincs elérhető dashboard</div>';
      }

      el.innerHTML = html;
    })
    .catch(function() {});
}

// ─── Directive & Priority quick actions ────────────────
function sendDirective(type) {
  var recipient = document.getElementById("recipientSelect").value || "";
  var payload = {};
  var content = "";
  var pri = 5;

  switch(type) {
    case "wake":
      content = "⚡ Ébresztés — kérlek jelentsd a státuszodat!";
      payload = {action: "wake", request: "status_report"};
      pri = 7;
      break;
    case "status":
      content = "📊 Státusz lekérdezés — küldd el a rendszerállapotod!";
      payload = {action: "status", request: "full_status"};
      pri = 5;
      break;
    case "ping":
      content = "📡 Ping teszt — válaszolj ha élsz!";
      payload = {action: "ping"};
      pri = 3;
      break;
    case "health":
      content = "❤️ Egészség jelentés — küldd el a health check adataidat!";
      payload = {action: "health_check", request: "health_report"};
      pri = 7;
      break;
    case "skills":
      content = "🛠️ Skills lekérdezés — listázd a képességeid!";
      payload = {action: "skills_query", request: "skills_list"};
      pri = 3;
      break;
    case "discover":
      content = "🔍 Agent felfedezés — derítsd fel a hálózat tagjait!";
      payload = {action: "discover", request: "network_discovery"};
      pri = 5;
      break;
    default:
      content = "Direktíva: " + type;
      payload = {action: type};
  }

  // Send as a directive message
  var msg = {
    sender: nodeId || authUser.display_name,
    recipient: recipient || "broadcast",
    content: content,
    type: "directive",
    priority: pri,
    payload: payload,
    timestamp: new Date().toISOString(),
    source: "web_dashboard"
  };

  // POST to /api/send
  fetch("/api/send", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Authorization": "Bearer " + (localStorage.getItem("mesh_token") || "")
    },
    body: JSON.stringify(msg)
  })
  .then(function(r) { return r.json(); })
  .then(function(d) {
    // Also display locally
    addMessageToDOM(msg);
    scrollToBottom();
  })
  .catch(function(e) {
    console.error("Directive send failed:", e);
  });
}

function setPriority(level) {
  document.getElementById("prioritySelect").value = level;
  // Visual feedback
  var sel = document.getElementById("prioritySelect");
  sel.style.borderColor = level >= 7 ? "var(--danger)" : level >= 4 ? "var(--warning)" : "var(--success)";
  setTimeout(function() { sel.style.borderColor = ""; }, 1500);
}

// ─── Recipient select changes channel ──────────────────
document.getElementById("recipientSelect").addEventListener("change", function() {
  var val = this.value;
  if (val === "") {
    switchChannel("general");
  } else {
    switchChannel(val);
  }
});

// ─── File Transfer Functions ──────────────────────────
function uploadFile() {
  var input = document.getElementById("fileInput");
  if (!input.files || !input.files.length) return;
  var recipient = document.getElementById("recipientSelect").value || "";
  uploadFiles(input.files, recipient);
  input.value = "";  // Reset for next upload
}

function uploadFileFromModal(input) {
  if (!input.files || !input.files.length) return;
  uploadFiles(input.files, "");
  input.value = "";
}

function uploadFiles(files, recipient) {
  var progressEl = document.getElementById("uploadProgress");
  var statusEl = document.getElementById("uploadStatus");
  var fillEl = document.getElementById("uploadFill");

  for (var i = 0; i < files.length; i++) {
    (function(file, idx) {
      var formData = new FormData();
      formData.append("file", file);
      if (recipient) formData.append("recipient", recipient);

      progressEl.style.display = "block";
      statusEl.textContent = "⬆️ Feltöltés: " + file.name + " (" + formatFileSize(file.size) + ")...";
      fillEl.style.width = "0%";

      var xhr = new XMLHttpRequest();
      xhr.open("POST", "/api/send-file", true);
      xhr.setRequestHeader("Authorization", "Bearer " + (localStorage.getItem("mesh_token") || ""));

      xhr.upload.onprogress = function(e) {
        if (e.lengthComputable) {
          var pct = Math.round((e.loaded / e.total) * 100);
          fillEl.style.width = pct + "%";
        }
      };

      xhr.onload = function() {
        fillEl.style.width = "100%";
        try {
          var data = JSON.parse(xhr.responseText);
          if (data.error) {
            statusEl.textContent = "❌ Hiba: " + data.error;
            statusEl.style.color = "var(--danger)";
          } else {
            statusEl.textContent = "✅ " + data.filename + " elküldve!";
            statusEl.style.color = "var(--success)";
            log("📎 Fájl elküldve: " + data.filename + " (" + formatFileSize(data.size) + ")");
            setTimeout(function() { progressEl.style.display = "none"; statusEl.style.color = ""; }, 3000);
            loadFileList();
          }
        } catch(e) {
          statusEl.textContent = "❌ Válasz hiba";
          statusEl.style.color = "var(--danger)";
        }
      };

      xhr.onerror = function() {
        statusEl.textContent = "❌ Hálózati hiba";
        statusEl.style.color = "var(--danger)";
      };

      xhr.send(formData);
    })(files[i], i);
  }
}

function formatFileSize(bytes) {
  if (bytes < 1024) return bytes + " B";
  if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + " KB";
  if (bytes < 1024 * 1024 * 1024) return (bytes / (1024 * 1024)).toFixed(1) + " MB";
  return (bytes / (1024 * 1024 * 1024)).toFixed(1) + " GB";
}

function showFileList() {
  document.getElementById("fileModal").style.display = "flex";
  loadFileList();
}

function loadFileList() {
  fetch("/api/files", {headers: {"Authorization": "Bearer " + (localStorage.getItem("mesh_token") || "")}})
    .then(function(r) { return r.json(); })
    .then(function(d) {
      var el = document.getElementById("fileListContent");
      if (!d.files || d.files.length === 0) {
        el.innerHTML = '<li style="text-align:center;color:var(--text3);padding:20px">Nincsenek fájlok</li>';
        return;
      }
      el.innerHTML = d.files.map(function(f) {
        var icon = f.name.match(/\.(png|jpg|jpeg|gif|webp|svg)$/i) ? "🖼️" :
                   f.name.match(/\.(mp4|avi|mkv|mov)$/i) ? "🎬" :
                   f.name.match(/\.(mp3|wav|ogg|flac)$/i) ? "🎵" :
                   f.name.match(/\.(pdf)$/i) ? "📄" :
                   f.name.match(/\.(zip|tar|gz|rar|7z)$/i) ? "📦" : "📎";
        var time = new Date(f.modified * 1000).toLocaleString("hu-HU");
        var typeLabel = f.type === "uploaded" ? "⬆️" : "⬇️";
        return '<li>' +
          '<div>' +
            '<div class="file-name">' + icon + ' ' + escapeHtml(f.name.replace(/^\d+_/, "")) + '</div>' +
            '<div class="file-meta">' + typeLabel + ' ' + f.size_human + ' · ' + time + '</div>' +
          '</div>' +
          '<div class="file-actions">' +
            '<a href="' + f.url + '?token=' + encodeURIComponent(localStorage.getItem("mesh_token") || "") + '" target="_blank" class="btn btn-sm" style="text-decoration:none">⬇️ Letöltés</a>' +
          '</div>' +
        '</li>';
      }).join('');
    })
    .catch(function() {
      document.getElementById("fileListContent").innerHTML = '<li style="text-align:center;color:var(--danger)">Hiba a fájlok betöltésekor</li>';
    });
}

// ─── Registry Functions ──────────────────────────────────
function showRegistry() {
  document.getElementById("registryModal").style.display = "flex";
  document.getElementById("registerAgentForm").style.display = "none";
  loadRegistry();
}

function showRegisterAgent() {
  document.getElementById("registerAgentForm").style.display = "block";
}

function registerAgent() {
  var name = document.getElementById("regName").value.trim();
  if (!name) { alert("Agent neve kötelező!"); return; }
  var data = {
    name: name,
    version: document.getElementById("regVersion").value.trim() || "1.0.0",
    endpoint: document.getElementById("regEndpoint").value.trim(),
    capabilities: document.getElementById("regCapabilities").value.split(",").map(function(s){return s.trim();}).filter(Boolean),
    description: document.getElementById("regDescription").value.trim(),
    force: true
  };
  fetch("/api/registry/agents", {
    method: "POST",
    headers: {"Content-Type": "application/json", "Authorization": "Bearer " + (localStorage.getItem("mesh_token") || "")},
    body: JSON.stringify(data)
  })
  .then(function(r) { return r.json(); })
  .then(function(d) {
    if (d.error) { alert("Hiba: " + d.error); }
    else { loadRegistry(); document.getElementById("registerAgentForm").style.display = "none"; }
  })
  .catch(function() { alert("Hálózati hiba"); });
}

function loadRegistry() {
  fetch("/api/registry", {headers: {"Authorization": "Bearer " + (localStorage.getItem("mesh_token") || "")}})
    .then(function(r) { return r.json(); })
    .then(function(d) {
      var statsEl = document.getElementById("registryStats");
      statsEl.innerHTML = "🟢 Egészséges: " + (d.healthy_agents||0) + " · 🟡 Degraded: " + (d.degraded_agents||0) + " · 🔴 Unhealthy: " + (d.unhealthy_agents||0) + " · Összesen: " + d.total_agents;
      loadRegistryAgents();
    })
    .catch(function() { document.getElementById("registryStats").textContent = "Hiba a statisztikák betöltésekor"; });
}

function loadRegistryAgents() {
  fetch("/api/registry/agents", {headers: {"Authorization": "Bearer " + (localStorage.getItem("mesh_token") || "")}})
    .then(function(r) { return r.json(); })
    .then(function(d) {
      var el = document.getElementById("registryList");
      if (!d.agents || d.agents.length === 0) {
        el.innerHTML = '<li style="text-align:center;color:var(--text3);padding:20px">Nincsenek regisztrált agensek</li>';
        return;
      }
      el.innerHTML = d.agents.map(function(a) {
        var scorePct = Math.round(a.health_score * 100);
        var scoreColor = scorePct >= 80 ? "var(--success)" : scorePct >= 50 ? "#f0ad4e" : "var(--danger)";
        var statusIcon = a.status === "healthy" ? "🟢" : a.status === "degraded" ? "🟡" : a.status === "unhealthy" ? "🔴" : "⚪";
        var caps = a.capabilities.map(function(c) { return '<span style="background:var(--primary);color:#fff;padding:2px 6px;border-radius:4px;font-size:11px;margin:2px">' + escapeHtml(c) + '</span>'; }).join(" ");
        return '<li style="flex-direction:column;align-items:stretch">' +
          '<div style="display:flex;justify-content:space-between;align-items:center">' +
            '<div><strong>' + statusIcon + ' ' + escapeHtml(a.name) + '</strong> <span style="color:var(--text3);font-size:12px">v' + escapeHtml(a.version) + '</span></div>' +
            '<div style="display:flex;align-items:center;gap:8px">' +
              '<div style="background:var(--border);border-radius:4px;width:80px;height:8px;overflow:hidden"><div style="background:' + scoreColor + ';height:100%;width:' + scorePct + '%;border-radius:4px"></div></div>' +
              '<span style="font-size:12px;color:' + scoreColor + '">' + scorePct + '%</span>' +
              '<button class="btn btn-sm" style="font-size:11px;padding:2px 8px" onclick="deregisterAgent(\'' + escapeHtml(a.name) + '\')">✕</button>' +
            '</div>' +
          '</div>' +
          (a.description ? '<div style="font-size:12px;color:var(--text3);margin-top:4px">' + escapeHtml(a.description) + '</div>' : '') +
          '<div style="margin-top:6px">' + (caps || '<span style="color:var(--text3);font-size:11px">Nincs képesség</span>') + '</div>' +
          '<div style="font-size:11px;color:var(--text3);margin-top:4px">' +
            '⏱️ ' + a.avg_latency_ms.toFixed(0) + 'ms · ' +
            '✅ ' + (a.success_rate * 100).toFixed(0) + '% · ' +
            '📊 ' + a.current_load + '/' + a.max_concurrent + ' · ' +
            '📈 ' + a.uptime_pct.toFixed(0) + '% · ' +
            '📨 ' + a.total_requests +
          '</div>' +
        '</li>';
      }).join('');
    })
    .catch(function() {
      document.getElementById("registryList").innerHTML = '<li style="text-align:center;color:var(--danger)">Hiba a registry betöltésekor</li>';
    });
}

function searchRegistry() {
  var caps = document.getElementById("registryCapSearch").value;
  var params = "capabilities=" + encodeURIComponent(caps);
  fetch("/api/registry/find?" + params, {headers: {"Authorization": "Bearer " + (localStorage.getItem("mesh_token") || "")}})
    .then(function(r) { return r.json(); })
    .then(function(d) {
      var el = document.getElementById("registryList");
      if (!d.matches || d.matches.length === 0) {
        el.innerHTML = '<li style="text-align:center;color:var(--text3);padding:20px">Nincs találat</li>';
        return;
      }
      el.innerHTML = d.matches.map(function(a) {
        var scorePct = Math.round(a.health_score * 100);
        var scoreColor = scorePct >= 80 ? "var(--success)" : scorePct >= 50 ? "#f0ad4e" : "var(--danger)";
        var caps = a.capabilities.map(function(c) { return '<span style="background:var(--primary);color:#fff;padding:2px 6px;border-radius:4px;font-size:11px;margin:2px">' + escapeHtml(c) + '</span>'; }).join(" ");
        return '<li>' +
          '<div><strong>' + escapeHtml(a.name) + '</strong> <span style="font-size:12px;color:var(--text3)">v' + escapeHtml(a.version) + '</span></div>' +
          '<div style="margin-top:4px">' + caps + '</div>' +
          '<div style="font-size:11px;color:var(--text3);margin-top:4px">' +
            'Health: <span style="color:' + scoreColor + '">' + scorePct + '%</span> · Load: ' + a.current_load +
          '</div>' +
        '</li>';
      }).join('');
    })
    .catch(function() { alert("Keresési hiba"); });
}

function deregisterAgent(name) {
  if (!confirm("Biztosan eltávolítod: " + name + "?")) return;
  fetch("/api/registry/agents/" + encodeURIComponent(name), {
    method: "DELETE",
    headers: {"Authorization": "Bearer " + (localStorage.getItem("mesh_token") || "")}
  })
  .then(function(r) { return r.json(); })
  .then(function(d) { loadRegistry(); })
  .catch(function() { alert("Hiba az eltávolításkor"); });
}

// ─── Auto-refresh ──────────────────────────────────────
setInterval(loadAgents, 10000);
setInterval(loadStatus, 5000);
setInterval(function() { if (authToken) checkAdminPanel(); }, 15000);

// ─── Init ──────────────────────────────────────────────
var savedToken = localStorage.getItem("a2a_token");
if (savedToken) {
  fetch("/api/auth/me", {headers: {"Authorization": "Bearer " + savedToken}})
    .then(function(r) { return r.json(); }).then(function(d) {
      if (d.user) {
        authToken = savedToken; authUser = d.user;
        document.getElementById("authModal").style.display = "none";
        document.getElementById("userBadge").style.display = "flex";
        updateUserBadge();
        initWebSocket(); loadStatus(); loadMessages(); loadAgents(); checkAdminPanel(); renderOpenChatsBar(); window.refreshMentionAgents();
      } else { localStorage.removeItem("a2a_token"); localStorage.removeItem("mesh_token"); showAuth(); }
    }).catch(function() { showAuth(); });
} else { showAuth(); }

document.getElementById("authUsername").addEventListener("keydown", function(e) { if(e.key==="Enter") document.getElementById("authPassword").focus(); });
document.getElementById("authPassword").addEventListener("keydown", function(e) { if(e.key==="Enter") submitAuth(); });
document.getElementById("messageInput").addEventListener("keydown", function(e) {
  // If the command palette is open, Enter completes the command instead of sending
  if (e.key==="Enter" && document.getElementById("commandPalette")) { return; }
  if(e.key==="Enter") sendMessage();
});
// Main room input: Telegram-style /command palette (↑/↓/Tab/Enter navigation)
window.attachCommandAutocomplete(document.getElementById("messageInput"));
(function() {
  var mi = document.getElementById("messageInput");
  if (mi) {
    mi.addEventListener("input", function() { window.showCommandPalette(mi); });
    mi.addEventListener("blur", function() { setTimeout(window.hideCommandPalette, 200); });
  }
})();
// ── Main chat file attach (general/broadcast room) ──
(function() {
  var mainAttach = document.getElementById("mainAttachBtn");
  var mainFile = document.getElementById("mainFileInput");
  if (mainAttach && mainFile) {
    mainAttach.onclick = function() { mainFile.click(); };
    mainFile.onchange = function() {
      var files = this.files;
      if (!files || !files.length) return;
      var token = localStorage.getItem("a2a_token") || localStorage.getItem("mesh_token") || "";
      // DM channel active? Send to that agent; else broadcast
      var ch = currentChannel || "general";
      var recipient = (ch !== "general") ? ch : "";
      var fd = new FormData();
      for (var i = 0; i < files.length; i++) fd.append("file", files[i]);
      fd.append("recipient", recipient);
      fd.append("message", "Fájl megosztva a chatben");
      mainAttach.textContent = "⏳";
      fetch("/api/send-file", {
        method: "POST",
        headers: { "Authorization": "Bearer " + token },
        body: fd
      }).then(function(r) {
        return r.text().then(function(t) { try { return JSON.parse(t); } catch(e) { throw new Error("Szerver válasz: " + r.status + " " + t.substring(0, 120)); } });
      }).then(function(d) {
        mainAttach.textContent = "📎";
        if (d.ok || d.status === 'ok' || d.file_name || d.filename) {
          loadMessages();
          if (ch !== "general" && typeof window._loadChatMessages === "function") window._loadChatMessages(ch, true);
        } else {
          alert("Fájl feltöltés hiba: " + (d.error || "ismeretlen"));
        }
        mainFile.value = "";
      }).catch(function(e) {
        mainAttach.textContent = "📎";
        alert("Fájl feltöltés hiba: " + e.message);
        mainFile.value = "";
      });
    };
  }
})();

function loadMarveenPage(page, params = '') {
  var titleMap = {
    'overview': '📊 Áttekintés',
    'kanban': '📋 Kanban Táblák',
    'approvals': '✅ Jóváhagyások',
    'agents': '🤖 Ügynökök',
    'activity': '📈 Aktivitás',
    'messages': '💬 Üzenetek',
    'tasks': '⏰ Ütemezések',
    'bgTasks': '🔄 Háttér Feladatok',
    'memories': '🧠 Memória',
    'naplo': '📝 Napló',
    'skills': '⭐ Skillek',
    'research': '💡 Ötletláda',
    'costs': '💰 Költségek',
    'tokenUsage': '🔢 Token Monitor',
    'updates': '⬆️ Frissítések',
    'vault': '🔐 Vault',
    'connectors': '🔌 MCP Registry',
    'federation': '🌐 Föderáció',
    'migrate': '📦 Költöztetés',
    'docs': '📚 Dokumentáció',
    'insights-cost': '💰 Költség',
    'insights-inbox': '📥 Beérkező',
    'insights-context-gate': '🧠 Kontextus Kapu',
    'insights-conversations': '💬 Beszélgetések',
    'insights-dream': '🌙 Dream Engine',
    'diagnostics': '🔧 Diagnosztika',
    'delegations': '📋 Delegációk',
    'alerts': '🔔 Riasztások',
    'health': '❤️ Egészség',
    'topology': '🕸️ Mesh Topológia',
    'governance': '⚖️ Governance',
    'watchers': '👁️ Watcherek',
    'routing': '🔀 Routing & Model',
    'projects': '📂 Projektek',
    'network': '🔌 P2P Hálózat',
    'security': '🛡️ Biztonság',
    'sysinfo': '⚙️ Rendszer Infó',
    'nodes': '🌐 Node-ok',
    'ideas': '💡 Ötletlád',
    'labels': '🏷️ Címkék',
    'shared-context': '📎 Shared Context',
    'files': '📁 Fájlok',
    'workflow': '⚙️ Workflow',
    'chat': '💬 Chat'
  };

  var apiMap = {
    'overview': '/api/overview',
    'kanban': '/api/kanban',
    'approvals': '/api/approvals',
    'agents': '/api/agents-page',
    'activity': '/api/activity',
    'messages': '/api/messages-page',
    'tasks': '/api/tasks',
    'bgTasks': '/api/cron',
    'memories': '/api/memory-page',
    'naplo': '/api/logs-page',
    'skills': '/api/skills',
    'research': '/api/ideas',
    'costs': '/api/costops/summary',
    'tokenUsage': '/api/token-usage',
    'updates': '/api/update-checker',
    'vault': '/api/vault/list',
    'connectors': '/api/mcp-registry',
    'federation': '/api/federation',
    'migrate': '/api/migrate',
    'docs': '/api/docs',
    'insights-cost': '/api/insights/cost',
    'insights-inbox': '/api/insights/inbox',
    'insights-context-gate': '/api/insights/context-gate',
    'insights-conversations': 'none',
    'insights-dream': '/api/insights/dream',
    'diagnostics': '/api/diagnostics',
    'delegations': '/api/delegations',
    'alerts': '/api/alerts',
    'health': '/api/health/scores',
    'topology': '/api/mesh/topology',
    'governance': '/api/governance/rules',
    'watchers': '/api/stuck-watcher',
    'routing': '/api/router/stats',
    'projects': '/api/projects',
    'network': '/api/p2p/status',
    'security': '/api/context-guard',
    'sysinfo': '/api/tool-timeouts',
    'nodes': '/api/nodes',
    'ideas': '/api/ideas',
    'labels': '/api/labels',
    'shared-context': '/api/context',
    'files': '/api/files',
    'workflow': '/api/workflows',
    'chat': '/api/chat/contacts'
  };

  var title = titleMap[page] || page;
  var apiPath = apiMap[page];
  var token = localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || '';

  // Create or show modal — always rebuild inner content to ensure structure is correct
  var modal = document.getElementById('marveenModal');
  if (modal) {
    // Remove old modal to force clean rebuild
    modal.remove();
    modal = null;
  }
  if (!modal) {
    modal = document.createElement('div');
    modal.id = 'marveenModal';
    modal.className = 'marveen-modal-overlay';
    var modalInner = document.createElement('div');
    modalInner.className = 'marveen-modal';

    var modalHeader = document.createElement('div');
    modalHeader.className = 'marveen-modal-header';

    var modalTitle = document.createElement('h2');
    modalTitle.id = 'marveenModalTitle';

    var closeBtn = document.createElement('button');
    closeBtn.className = 'marveen-modal-close';
    closeBtn.innerHTML = '✕';
    closeBtn.title = 'Bezárás (Esc)';
    closeBtn.onclick = closeMarveenModal;

    modalHeader.appendChild(modalTitle);
    modalHeader.appendChild(closeBtn);

    var modalBody = document.createElement('div');
    modalBody.className = 'marveen-modal-body';
    modalBody.id = 'marveenModalBody';

    modalInner.appendChild(modalHeader);
    modalInner.appendChild(modalBody);
    modal.appendChild(modalInner);
    document.body.appendChild(modal);
    // Close on backdrop click
    modal.addEventListener('click', function(e) { if (e.target === modal) closeMarveenModal(); });
    // Close on Esc
    document.addEventListener('keydown', function(e) { if (e.key === 'Escape') closeMarveenModal(); });
  }

  document.getElementById('marveenModalTitle').innerHTML = title;
  var body = document.getElementById('marveenModalBody');
  body.innerHTML = '<div style="text-align:center;padding:40px;color:var(--text3)"><div style="font-size:32px;margin-bottom:12px">⏳</div>Betöltés...</div>';
  modal.style.display = 'flex';

  // ── Per-page renderers ──
  function esc(s) { return String(s == null ? '' : s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;'); }
  function fmtTime(ts) {
    if (!ts) return '<span style="color:var(--text3)">—</span>';
    var d = new Date(ts);
    if (isNaN(d)) return esc(ts);
    return d.toLocaleString('hu-HU', {month:'short',day:'numeric',hour:'2-digit',minute:'2-digit'});
  }
  function badge(text, color) {
    var c = color || 'var(--primary)';
    return '<span style="display:inline-block;padding:2px 8px;border-radius:10px;font-size:11px;font-weight:600;background:' + c + '22;color:' + c + ';">' + esc(text) + '</span>';
  }
  function card(inner) {
    return '<div style="background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:14px;margin-bottom:10px;">' + inner + '</div>';
  }
  function table(headers, rows) {
    var h = '<table style="width:100%;border-collapse:collapse;font-size:13px;"><thead><tr>';
    headers.forEach(function(hd) { h += '<th style="text-align:left;padding:8px 10px;border-bottom:2px solid var(--border);color:var(--text3);font-size:11px;text-transform:uppercase;letter-spacing:.5px;">' + esc(hd) + '</th>'; });
    h += '</tr></thead><tbody>';
    rows.forEach(function(row) {
      h += '<tr style="border-bottom:1px solid var(--border)">';
      row.forEach(function(cell) { h += '<td style="padding:8px 10px;color:var(--text2);">' + cell + '</td>'; });
      h += '</tr>';
    });
    h += '</tbody></table>';
    return h;
  }
  function empty(msg) {
    return '<div style="text-align:center;padding:40px;color:var(--text3);font-size:14px;">' + (msg || 'Nincs adat') + '</div>';
  }
  function errorBox(msg) {
    return '<div style="color:var(--danger);text-align:center;padding:40px;">' +
      '<div style="font-size:32px;margin-bottom:12px">⚠️</div>' +
      '<div style="font-size:14px;font-weight:600;margin-bottom:6px">Hiba</div>' +
      '<div style="font-size:13px;color:var(--text3)">' + esc(msg) + '</div></div>';
  }

  // ─── MCP detail modal (exposed to global scope) ───
  window.showMcpDetail = function(name, node, detailStr) {
    var s = typeof detailStr === 'string' ? JSON.parse(detailStr.replace(/&quot;/g, '"').replace(/&#39;/g, "'")) : detailStr;
    var modal = document.getElementById('marveenModal');
    var title = document.getElementById('marveenModalTitle');
    var body = document.getElementById('marveenModalBody');
    if (!modal || !body) return;
    title.textContent = '🔌 ' + name + ' — MCP Részletek';
    var transport = s.transport || 'stdio';
    var html = '<div style="padding:16px">';
    html += '<div style="margin-bottom:12px"><strong style="color:var(--text)">Szerver:</strong> ' + esc(s.name || '?') + '</div>';
    html += '<div style="margin-bottom:12px"><strong style="color:var(--text)">Node:</strong> ' + esc(node) + '</div>';
    html += '<div style="margin-bottom:12px"><strong style="color:var(--text)">Transport:</strong> ' + (transport === 'streamable_http' ? '🌐 HTTP' : '📦 STDIO') + '</div>';
    if (s.url) html += '<div style="margin-bottom:12px"><strong style="color:var(--text)">URL:</strong><br><code style="font-size:11px;word-break:break-all">' + esc(s.url) + '</code></div>';
    if (s.command) {
      var argsStr = Array.isArray(s.args) ? s.args.join(' ') : (typeof s.args === 'string' ? s.args : '');
      html += '<div style="margin-bottom:12px"><strong style="color:var(--text)">Parancs:</strong><br><code style="font-size:11px">' + esc(s.command) + ' ' + esc(argsStr) + '</code></div>';
    }
    html += '<div style="margin-bottom:12px"><strong style="color:var(--text)">Státusz:</strong> ' + (s.enabled !== false ? '✅ aktív' : '⏸️ inaktív') + '</div>';
    if (s.env_keys && s.env_keys.length) {
      html += '<div style="margin-bottom:12px"><strong style="color:var(--text)">Env változók:</strong><br>';
      s.env_keys.forEach(function(k) { html += '<code style="font-size:11px;margin-right:8px">' + esc(k) + '</code>'; });
      html += '</div>';
    }
    if (s.has_credentials) {
      html += '<div style="margin-bottom:12px;padding:8px;background:var(--warning);color:#000;border-radius:6px;font-size:11px">⚠️ Ez a szerver hitelesítési adatokat igényel — a beépítés után be kell állítani az env változókat!</div>';
    }
    // Config preview
    var config = {};
    if (s.url) config.url = s.url;
    if (s.command) { config.command = s.command; config.args = s.args || []; }
    config.enabled = true;
    html += '<div style="margin-bottom:12px"><strong style="color:var(--text)">config.yaml bejegyzés:</strong><pre style="background:var(--surface2);padding:8px;border-radius:6px;font-size:10px;overflow-x:auto">' + esc(JSON.stringify(config, null, 2)) + '</pre></div>';
    // Install button
    if (s.url || s.command) {
      html += '<button onclick="installMcpServer(\'' + esc(s.name) + '\', \'' + esc(s.url || '') + '\', \'' + esc(node) + '\', \'' + esc(s.command || '') + '\', ' + JSON.stringify(s.args || []).replace(/'/g, "&#39;") + ')" ' +
        'style="background:var(--primary);color:#fff;border:none;padding:8px 16px;border-radius:6px;cursor:pointer;width:100%;font-size:13px">🔌 Beépítem a lokális config-ba</button>';
    }
    html += '</div>';
    body.innerHTML = html;
    modal.style.display = 'flex';
  };

  // ─── Install MCP server to local config (exposed to global scope) ───
  window.installMcpServer = function(name, url, sourceNode, command, args) {
    var config = {enabled: true};
    if (url) {
      config.url = url;
      config.transport = 'streamable_http';
    } else if (command) {
      config.command = command;
      config.args = args || [];
    }
    var token = localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || '';
    var body = JSON.stringify({name: name + '-from-' + sourceNode, config: config});
    fetch('/api/mcp-install', {
      method: 'POST',
      headers: {'Content-Type': 'application/json', 'Authorization': 'Bearer ' + token},
      body: body
    }).then(function(r) { return r.json(); })
    .then(function(d) {
      if (d.success) {
        alert('✅ MCP \'' + name + '\' beépítve!\n\n' + d.message + '\n\nBackup: ' + d.backup);
        loadMarveenPage('connectors');  // refresh
      } else {
        alert('❌ Hiba: ' + (d.error || 'ismeretlen'));
      }
    }).catch(function(e) {
      alert('❌ Hálózati hiba: ' + e.message);
    });
  };

  // ─── Memory detail modal (exposed to global scope for onclick) ───
  window.showMemoryDetail = function(mid) {
    // Find memory in current data by id
    var token = localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || '';
    fetch('/api/memory-page', { headers: { 'Authorization': 'Bearer ' + token } })
      .then(function(r) { return r.json(); })
      .then(function(d) {
        var mem = null;
        (d.memories || []).forEach(function(m) { if (m.id === mid) mem = m; });
        if (!mem) { alert('Memória nem található'); return; }
        var body = document.getElementById('marveenModalBody');
        if (!body) return;
        var content = mem.content || '';
        var contentHtml;
        try {
          var parsed = JSON.parse(content);
          contentHtml = '<pre style="white-space:pre-wrap;font-size:12px;color:var(--text2);background:var(--surface2);padding:12px;border-radius:8px;overflow-x:auto;">' + esc(JSON.stringify(parsed, null, 2)) + '</pre>';
        } catch(e) {
          contentHtml = '<div style="font-size:13px;color:var(--text2);line-height:1.6;white-space:pre-wrap;">' + esc(content) + '</div>';
        }
        body.innerHTML =
          '<div style="margin-bottom:12px;">' +
          '<div style="font-size:11px;color:var(--text3);margin-bottom:4px;">Típus: ' + esc(mem.memory_type || mem.message_type || '—') + '</div>' +
          '<div style="font-size:16px;font-weight:600;color:var(--text);margin-bottom:8px;">' + esc(mem.subject || '(nincs tárgy)') + '</div>' +
          '<div style="display:flex;gap:12px;font-size:12px;color:var(--text3);margin-bottom:12px;">' +
          '<span>📤 ' + esc(mem.sender_agent || '?') + '</span>' +
          '<span>📥 ' + esc(mem.recipient_agent || '?') + '</span>' +
          '<span>Priority: P' + (mem.priority || 5) + '</span>' +
          '<span>Status: ' + esc(mem.status || '—') + '</span>' +
          '</div></div>' + contentHtml +
          '<div style="margin-top:12px;font-size:11px;color:var(--text3);">ID: ' + esc(mem.id) + ' | Létrehozva: ' + fmtTime(mem.created_at) + '</div>';
        var title = document.getElementById('marveenModalTitle');
        if (title) title.textContent = '🧠 Memória részletek';
      }).catch(function(e) { alert('Hiba: ' + e); });
  }

  // ─── Log detail modal (exposed to global scope for onclick) ───
  window.showLogDetail = function(lid, detailJson) {
    var body = document.getElementById('marveenModalBody');
    if (!body) return;
    try {
      var l = JSON.parse(detailJson.replace(/&quot;/g, '"').replace(/&#39;/g, "'").replace(/\\'/g, "'"));
    } catch(e) {
      body.innerHTML = '<div style="color:var(--danger);">Nem sikerült elemezni a bejegyzést</div>';
      return;
    }
    var lt = l.log_type || 'delegation';
    var typeLabel = lt === 'health' ? '💊 Health Check' : '📋 Delegáció';
    var status = l.status || '—';
    var statusColor = status === 'completed' ? 'var(--success)' : status === 'failed' ? 'var(--danger)' : status === 'available' ? 'var(--warning)' : 'var(--text3)';
    // Build detail fields
    var fields = [];
    if (l.from_agent) fields.push(['Feladó', l.from_agent]);
    if (l.to_agent) fields.push(['Címzett', l.to_agent]);
    if (l.assigned_agent) fields.push(['Hozzárendelt', l.assigned_agent]);
    if (l.priority) fields.push(['Prioritás', 'P' + l.priority]);
    if (l.retry_count !== undefined) fields.push(['Újrapróbálkozások', l.retry_count]);
    if (l.task_type) fields.push(['Típus', l.task_type]);
    if (l.node_name) fields.push(['Node', l.node_name]);
    if (l.cpu_pct !== undefined) fields.push(['CPU', l.cpu_pct + '%']);
    if (l.memory_pct !== undefined) fields.push(['Memória', l.memory_pct + '%']);
    if (l.disk_pct !== undefined) fields.push(['Lemez', l.disk_pct + '%']);
    if (l.created_at) fields.push(['Létrehozva', fmtTime(l.created_at)]);
    if (l.completed_at) fields.push(['Befejezve', fmtTime(l.completed_at)]);
    if (l.last_seen) fields.push(['Utolsó jel', fmtTime(l.last_seen)]);
    if (l.updated_at) fields.push(['Frissítve', fmtTime(l.updated_at)]);
    var fieldsHtml = '';
    fields.forEach(function(f) {
      fieldsHtml += '<div style="display:flex;gap:8px;padding:6px 0;border-bottom:1px solid var(--border);"><span style="color:var(--text3);min-width:120px;font-size:12px;">' + esc(f[0]) + '</span><span style="color:var(--text);font-size:13px;">' + esc(String(f[1])) + '</span></div>';
    });
    body.innerHTML =
      '<div style="margin-bottom:12px;">' +
      '<div style="font-size:11px;color:var(--text3);margin-bottom:4px;">' + typeLabel + '</div>' +
      '<div style="font-size:16px;font-weight:600;color:var(--text);margin-bottom:8px;">' + esc(l.subject || l.node_name || '(nincs)') + '</div>' +
      '<span style="font-size:10px;padding:2px 8px;border-radius:4px;background:' + statusColor + ';color:#fff;font-weight:600;text-transform:uppercase;">' + esc(status) + '</span>' +
      '</div>' + fieldsHtml;
    var title = document.getElementById('marveenModalTitle');
    if (title) title.textContent = '📋 Napló bejegyzés';
  }

  // ─── Log filtering + export (exposed to global scope) ───
  window.filterLogs = function(type) {
    var token = localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || '';
    var params = 'type=' + type + '&limit=50';
    var searchEl = document.getElementById('logSearchInput');
    var nodeEl = document.getElementById('logNodeFilter');
    var statusEl = document.getElementById('logStatusFilter');
    var fromEl = document.getElementById('logDateFrom');
    var toEl = document.getElementById('logDateTo');
    if (searchEl && searchEl.value) params += '&q=' + encodeURIComponent(searchEl.value);
    if (nodeEl && nodeEl.value) params += '&node=' + encodeURIComponent(nodeEl.value);
    if (statusEl && statusEl.value) params += '&status=' + encodeURIComponent(statusEl.value);
    if (fromEl && fromEl.value) params += '&from=' + encodeURIComponent(fromEl.value);
    if (toEl && toEl.value) params += '&to=' + encodeURIComponent(toEl.value);
    fetch('/api/logs-page?' + params, { headers: { 'Authorization': 'Bearer ' + token } })
      .then(function(r) { return r.json(); })
      .then(function(d) {
        var body = document.getElementById('marveenModalBody');
        if (body) {
          var renderer = renderers['naplo'];
          if (renderer) body.innerHTML = renderer(d);
        }
      })
      .catch(function(e) { alert('Hiba: ' + e.message); });
  };

  window.applyLogFilters = function() {
    // Read current filter type from active button (default: all)
    var type = 'all';
    var activeBtn = document.querySelector('#marveenModalBody span[style*="var(--primary)"]');
    if (activeBtn) {
      var onclick = activeBtn.getAttribute('onclick') || '';
      var m = onclick.match(/filterLogs\('(\w+)'\)/);
      if (m) type = m[1];
    }
    window.filterLogs(type);
  };

  window.clearLogFilters = function() {
    var els = ['logSearchInput','logNodeFilter','logStatusFilter','logDateFrom','logDateTo'];
    for (var i = 0; i < els.length; i++) {
      var el = document.getElementById(els[i]);
      if (el) el.value = '';
    }
    window.filterLogs('all');
  };

  window.exportLogs = function(format) {
    var token = localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || '';
    var params = 'export=' + format + '&limit=500';
    var searchEl = document.getElementById('logSearchInput');
    var nodeEl = document.getElementById('logNodeFilter');
    var statusEl = document.getElementById('logStatusFilter');
    var fromEl = document.getElementById('logDateFrom');
    var toEl = document.getElementById('logDateTo');
    if (searchEl && searchEl.value) params += '&q=' + encodeURIComponent(searchEl.value);
    if (nodeEl && nodeEl.value) params += '&node=' + encodeURIComponent(nodeEl.value);
    if (statusEl && statusEl.value) params += '&status=' + encodeURIComponent(statusEl.value);
    if (fromEl && fromEl.value) params += '&from=' + encodeURIComponent(fromEl.value);
    if (toEl && toEl.value) params += '&to=' + encodeURIComponent(toEl.value);
    var url = '/api/logs-page?' + params;
    fetch(url, { headers: { 'Authorization': 'Bearer ' + token } })
      .then(function(r) { return r.blob ? r.blob() : r.text(); })
      .then(function(blob) {
        var filename = format === 'csv' ? 'mesh_logs.csv' : 'mesh_logs.json';
        var blobObj = blob instanceof Blob ? blob : new Blob([blob], { type: format === 'csv' ? 'text/csv' : 'application/json' });
        var a = document.createElement('a');
        a.href = URL.createObjectURL(blobObj);
        a.download = filename;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        URL.revokeObjectURL(a.href);
      })
      .catch(function(e) { alert('Export hiba: ' + e.message); });
  };

  // ─── Message compose + detail (exposed to global scope) ───
  window.showComposeMessage = function(replyTo, prefillRecipient) {
    var body = document.getElementById('marveenModalBody');
    if (!body) return;
    var title = document.getElementById('marveenModalTitle');
    if (title) title.textContent = replyTo ? '↩️ Válasz üzenet' : '✏️ Új üzenet küldése';
    var peers = ['broadcast','nova','morzsa','runa','tor'];
    var types = ['a2a_message','chat','directive','delegation','diagnostic_report'];
    var html = '<div style="padding:4px;">';
    // Recipient
    html += '<label style="display:block;margin-bottom:6px;font-size:12px;color:var(--text3);">Címzett</label>';
    html += '<select id="composeRecipient" style="width:100%;padding:8px 10px;border-radius:8px;border:1px solid var(--border);background:var(--surface2);color:var(--text);font-size:13px;margin-bottom:12px;">';
    for (var i = 0; i < peers.length; i++) {
      html += '<option value="' + peers[i] + '"' + (prefillRecipient === peers[i] ? ' selected' : '') + '>' + (peers[i] === 'broadcast' ? '📡 Broadcast (minden node)' : '🤖 ' + peers[i]) + '</option>';
    }
    html += '</select>';
    // Message type
    html += '<label style="display:block;margin-bottom:6px;font-size:12px;color:var(--text3);">Üzenet típus</label>';
    html += '<select id="composeType" style="width:100%;padding:8px 10px;border-radius:8px;border:1px solid var(--border);background:var(--surface2);color:var(--text);font-size:13px;margin-bottom:12px;">';
    for (var j = 0; j < types.length; j++) {
      html += '<option value="' + types[j] + '">' + types[j] + '</option>';
    }
    html += '</select>';
    // Priority
    html += '<label style="display:block;margin-bottom:6px;font-size:12px;color:var(--text3);">Prioritás</label>';
    html += '<select id="composePriority" style="width:100%;padding:8px 10px;border-radius:8px;border:1px solid var(--border);background:var(--surface2);color:var(--text);font-size:13px;margin-bottom:12px;">';
    html += '<option value="5">Normál (5)</option>';
    html += '<option value="1">Magas (1)</option>';
    html += '<option value="9">Alacsony (9)</option>';
    html += '</select>';
    // Message text
    html += '<label style="display:block;margin-bottom:6px;font-size:12px;color:var(--text3);">Üzenet szövege</label>';
    html += '<textarea id="composeText" rows="4" placeholder="Írd be az üzenetet..." style="width:100%;padding:10px;border-radius:8px;border:1px solid var(--border);background:var(--surface2);color:var(--text);font-size:13px;margin-bottom:12px;resize:vertical;font-family:inherit;"></textarea>';
    // Buttons
    html += '<div style="display:flex;gap:8px;">';
    html += '<button onclick="sendMessage()" style="background:var(--primary);color:#fff;border:none;padding:8px 24px;border-radius:10px;font-size:13px;cursor:pointer;font-weight:600;">📤 Küldés</button>';
    html += '<button onclick="loadMarveenPage(\'messages\')" style="background:var(--surface2);color:var(--text2);border:1px solid var(--border);padding:8px 24px;border-radius:10px;font-size:13px;cursor:pointer;">Mégse</button>';
    html += '</div>';
    html += '</div>';
    body.innerHTML = html;
  };

  window.sendMessage = function() {
    var token = localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || '';
    var recipient = document.getElementById('composeRecipient').value;
    var msgType = document.getElementById('composeType').value;
    var priority = parseInt(document.getElementById('composePriority').value, 10);
    var text = document.getElementById('composeText').value.trim();
    if (!text) { alert('Az üzenet szövege nem lehet üres!'); return; }
    var btn = event.target;
    if (btn) { btn.disabled = true; btn.textContent = '⏳ Küldés...'; }
    fetch('/api/send', {
      method: 'POST',
      headers: { 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json' },
      body: JSON.stringify({ recipient: recipient, msg_type: msgType, text: text, priority: priority })
    })
      .then(function(r) { return r.json(); })
      .then(function(d) {
        if (d.ok) {
          alert('✅ Üzenet elküldve! (' + recipient + ')');
          loadMarveenPage('messages');
        } else {
          alert('❌ Hiba: ' + (d.error || 'ismeretlen'));
          if (btn) { btn.disabled = false; btn.textContent = '📤 Küldés'; }
        }
      })
      .catch(function(e) {
        alert('❌ Hiba: ' + e.message);
        if (btn) { btn.disabled = false; btn.textContent = '📤 Küldés'; }
      });
  };

  window.showMessageDetail = function(msgId) {
    var token = localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || '';
    var body = document.getElementById('marveenModalBody');
    if (!body) return;
    var title = document.getElementById('marveenModalTitle');
    if (title) title.textContent = '📨 Üzenet részletek';
    body.innerHTML = '<div style="text-align:center;padding:40px;color:var(--text3);">⏳ Betöltés...</div>';
    fetch('/api/messages/detail/' + encodeURIComponent(msgId), { headers: { 'Authorization': 'Bearer ' + token } })
      .then(function(r) { return r.json(); })
      .then(function(d) {
        var m = d.message;
        if (!m) { body.innerHTML = errorBox('Üzenet nem található'); return; }
        var payload = m.payload || {};
        if (typeof payload === 'string') { try { payload = JSON.parse(payload); } catch(e) {} }
        var payloadStr = '';
        try { payloadStr = JSON.stringify(payload, null, 2); } catch(e) { payloadStr = String(payload); }
        var fields = [
          ['ID', m.id],
          ['Feladó', m.sender],
          ['Címzett', m.recipient],
          ['Típus', m.msg_type],
          ['Prioritás', m.priority],
          ['Státusz', m.status],
          ['Idő', fmtTime(m.created_at)]
        ];
        var html = '';
        fields.forEach(function(f) {
          html += '<div style="display:flex;gap:8px;padding:6px 0;border-bottom:1px solid var(--border);"><span style="color:var(--text3);min-width:100px;font-size:12px;">' + esc(f[0]) + '</span><span style="color:var(--text);font-size:13px;word-break:break-all;">' + esc(String(f[1] || '—')) + '</span></div>';
        });
        // Payload
        html += '<div style="margin-top:12px;margin-bottom:6px;font-size:12px;color:var(--text3);">Payload</div>';
        html += '<pre style="background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:12px;font-size:11px;overflow-x:auto;white-space:pre-wrap;word-break:break-all;">' + esc(payloadStr) + '</pre>';
        // Reply button
        html += '<div style="margin-top:12px;">';
        html += '<button onclick="showComposeMessage(true,\'' + esc(m.sender || '') + '\')" style="background:var(--primary);color:#fff;border:none;padding:8px 20px;border-radius:10px;font-size:13px;cursor:pointer;font-weight:600;">↩️ Válasz</button>';
        html += '<button onclick="loadMarveenPage(\'messages\')" style="background:var(--surface2);color:var(--text2);border:1px solid var(--border);padding:8px 20px;border-radius:10px;font-size:13px;cursor:pointer;margin-left:8px;">◀ Vissza</button>';
        html += '</div>';
        body.innerHTML = html;
      })
      .catch(function(e) { body.innerHTML = errorBox(e.message); });
  };

  // ─── Kanban card actions (exposed to global scope) ───
  window.moveKanbanCard = function(boardId, cardId, newCol) {
    var token = localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || '';
    fetch('/api/kanban/' + boardId + '/cards/' + cardId, {
      method: 'PUT',
      headers: { 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json' },
      body: JSON.stringify({ column: newCol })
    })
      .then(function(r) { return r.json(); })
      .then(function(d) { if (d.error) { alert('Hiba: ' + d.error); } loadMarveenPage('kanban'); })
      .catch(function(e) { alert('Hiba: ' + e.message); });
  };

  // Kanban kártya részletes modal — ujjbarát oszlopváltás, mobilon is működik
  window.showKanbanCardDetail = function(boardId, cardId) {
    var cached = (window._kanbanCardsCache || {})[boardId + ':' + cardId];
    if (!cached) return;
    var c = cached.card;
    var cols = cached.columns;
    var col = cached.column;
    var colLabels = {'todo':'📋 Teendő','in_progress':'🔄 Folyamatban','review':'👀 Felülvizsgálat','done':'✅ Kész'};
    var colIdx = cols.indexOf(col);
    var pri = c.priority || 'medium';
    var pc = pri === 'high' ? 'var(--danger)' : pri === 'low' ? 'var(--text3)' : 'var(--warning)';

    var html = '<div style="display:flex;justify-content:space-between;align-items:flex-start;gap:8px;">';
    html += '<div><div style="font-size:16px;font-weight:700;color:var(--text);">' + esc(c.title || '—') + '</div>';
    html += '<div style="font-size:11px;color:var(--text3);margin-top:2px;">📍 ' + (colLabels[col] || col) + '</div></div>';
    html += '<button onclick="window._closeVaultModal()" style="background:none;border:none;color:var(--text3);font-size:22px;cursor:pointer;">✕</button></div>';
    if (c.description) html += '<div style="font-size:12px;color:var(--text2);margin:10px 0;white-space:pre-wrap;">' + esc(c.description) + '</div>';
    html += '<div style="display:flex;gap:6px;flex-wrap:wrap;margin:8px 0;">';
    html += '<span style="font-size:10px;padding:2px 8px;border-radius:4px;background:' + pc + ';color:#fff;font-weight:600;">' + esc(pri) + '</span>';
    if (c.assigned_to) html += '<span style="font-size:10px;color:var(--text2);">👤 ' + esc(c.assigned_to) + '</span>';
    if (c.due_date) html += '<span style="font-size:10px;color:var(--text2);">📅 ' + esc(c.due_date) + '</span>';
    html += '</div>';
    if (c.requires_approval || c.approval_status === 'pending') {
      html += '<button onclick="approveKanbanCard(\'' + boardId + '\',\'' + cardId + '\');closeIdeaModal();" style="width:100%;padding:12px;border:none;border-radius:8px;background:var(--success);color:#fff;font-weight:600;font-size:14px;cursor:pointer;margin-bottom:8px;">✓ Jóváhagyás</button>';
    }
    // Nagy, ujjbarát oszlopváltó gombok
    html += '<div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-top:10px;">';
    if (colIdx > 0) {
      html += '<button onclick="moveKanbanCard(\'' + boardId + '\',\'' + cardId + '\',\'' + cols[colIdx-1] + '\')" style="padding:14px;border:1px solid var(--border);border-radius:10px;background:var(--surface);color:var(--text);font-size:15px;cursor:pointer;">◀ ' + (colLabels[cols[colIdx-1]] || cols[colIdx-1]) + '</button>';
    }
    if (colIdx < cols.length-1) {
      html += '<button onclick="moveKanbanCard(\'' + boardId + '\',\'' + cardId + '\',\'' + cols[colIdx+1] + '\')" style="padding:14px;border:1px solid var(--border);border-radius:10px;background:var(--surface);color:var(--text);font-size:15px;cursor:pointer;">' + (colLabels[cols[colIdx+1]] || cols[colIdx+1]) + ' ▶</button>';
    }
    html += '</div>';
    html += '<button onclick="deleteKanbanCard(\'' + boardId + '\',\'' + cardId + '\');window._closeVaultModal()" style="width:100%;padding:10px;border:1px solid var(--danger);border-radius:8px;background:none;color:var(--danger);font-size:13px;cursor:pointer;margin-top:12px;">🗑️ Kártya törlése</button>';

    window._showVaultModal('📋 ' + (c.title || 'Kártya'), html, [
      { label: '✕ Bezárás', onclick: 'window._closeVaultModal()' }
    ]);
  };

  window.addKanbanCard = function(boardId) {
    var title = prompt('Kártya címe:');
    if (!title) return;
    var desc = prompt('Leírás (opcionális):') || '';
    var pri = prompt('Prioritás (low/medium/high):', 'medium') || 'medium';
    var assignee = prompt('Hozzárendelt agent (opcionális):') || '';
    var token = localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || '';
    fetch('/api/kanban/' + boardId + '/cards', {
      method: 'POST',
      headers: { 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json' },
      body: JSON.stringify({ title: title, description: desc, priority: pri, assigned_to: assignee, column: 'todo' })
    })
      .then(function(r) { return r.json(); })
      .then(function(d) { if (d.error) { alert('Hiba: ' + d.error); } loadMarveenPage('kanban'); })
      .catch(function(e) { alert('Hiba: ' + e.message); });
  };

  window.deleteKanbanCard = function(boardId, cardId) {
    if (!confirm('Biztosan törlöd ezt a kártyát?')) return;
    var token = localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || '';
    fetch('/api/kanban/' + boardId + '/cards/' + cardId, {
      method: 'DELETE',
      headers: { 'Authorization': 'Bearer ' + token }
    })
      .then(function(r) { return r.json(); })
      .then(function(d) { loadMarveenPage('kanban'); })
      .catch(function(e) { alert('Hiba: ' + e.message); });
  };

  window.approveKanbanCard = function(boardId, cardId) {
    var token = localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || '';
    fetch('/api/kanban/' + boardId + '/cards/' + cardId + '/approve', {
      method: 'POST',
      headers: { 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json' },
      body: JSON.stringify({ approved: true })
    })
      .then(function(r) { return r.json(); })
      .then(function(d) { if (d.error) { alert('Hiba: ' + d.error); } loadMarveenPage('kanban'); })
      .catch(function(e) { alert('Hiba: ' + e.message); });
  };

  // ─── Federation actions (exposed to global scope) ───
  window.federationAction = function(action, name) {
    var token = localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || '';
    var url = '/api/federation';
    var method = 'POST';
    var body = null;

    if (action === 'remove') {
      url += '/peer/' + encodeURIComponent(name);
      method = 'DELETE';
    } else if (action === 'connect') {
      body = JSON.stringify({name: name});
    } else if (action === 'discover') {
      // call discovery API, then reload
      fetch(url + '/discover', {
        method: 'POST',
        headers: {'Content-Type': 'application/json', 'Authorization': 'Bearer ' + token},
        body: '{}'
      }).then(function(r) { return r.json(); })
        .then(function(d) {
          if (d.discovered && d.discovered.length > 0) {
            // Auto-add discovered peers
            var added = 0;
            var total = d.discovered.length;
            d.discovered.forEach(function(node) {
              fetch('/api/federation/peer', {
                method: 'POST',
                headers: {'Content-Type': 'application/json', 'Authorization': 'Bearer ' + token},
                body: JSON.stringify({name: node.name, address: node.address, port: node.port})
              }).then(function() {
                added++;
                if (added >= total) { loadMarveenPage('federation'); }
              });
            });
          } else {
            loadMarveenPage('federation');
          }
        }).catch(function(e) { alert('Hiba: ' + e.message); loadMarveenPage('federation'); });
      return;
    } else if (action === 'trust') {
      url += '/trust/' + encodeURIComponent(name);
      body = JSON.stringify({level: 'toggle'}); // toggle trust on each click
    }

    fetch(url, {
      method: method,
      headers: {'Content-Type': 'application/json', 'Authorization': 'Bearer ' + token},
      body: body
    }).then(function(r) { return r.json(); })
      .then(function(d) {
        if (d.success) {
          loadMarveenPage('federation');
        } else {
          alert('❌ Hiba: ' + (d.error || 'ismeretlen'));
        }
      }).catch(function(e) { alert('❌ Hálózati hiba: ' + e.message); });
  };

  window.showFederationAddModal = function() {
    var body = document.getElementById('marveenModalBody');
    if (!body) return;
    var title = document.getElementById('marveenModalTitle');
    if (title) title.textContent = '🌐 Új Federált Peer';
    
    var html = '<div style=\"padding:16px;display:flex;flex-direction:column;gap:12px\">';
    html += '<div><label style=\"display:block;font-size:12px;color:var(--text3);margin-bottom:4px\">Név</label>';
    html += '<input type=\"text\" id=\"fedPeerName\" style=\"width:100%;background:var(--surface2);border:1px solid var(--border);border-radius:6px;padding:8px;color:var(--text);\"></div>';
    html += '<div><label style=\"display:block;font-size:12px;color:var(--text3);margin-bottom:4px\">Host / IP</label>';
    html += '<input type=\"text\" id=\"fedPeerHost\" style=\"width:100%;background:var(--surface2);border:1px solid var(--border);border-radius:6px;padding:8px;color:var(--text);\"></div>';
    html += '<div><label style=\"display:block;font-size:12px;color:var(--text3);margin-bottom:4px\">Port</label>';
    html += '<input type=\"text\" id=\"fedPeerPort\" value=\"8650\" style=\"width:100%;background:var(--surface2);border:1px solid var(--border);border-radius:6px;padding:8px;color:var(--text);\"></div>';
    html += '<div style=\"display:flex;align-items:center;gap:8px;margin-top:8px\">';
    html += '<input type=\"checkbox\" id=\"fedPeerSsh\" style=\"cursor:pointer;\"> <label for=\"fedPeerSsh\" style=\"font-size:12px;color:var(--text);cursor:pointer;\">SSH Tünnel használata</label></div>';
    html += '<div style=\"display:flex;gap:8px;justify-content:flex-end;margin-top:16px\">';
    html += '<button onclick=\"closeMarveenModal()\" style=\"background:var(--surface2);color:var(--text);border:1px solid var(--border);padding:8px 16px;border-radius:6px;cursor:pointer;\">Mégsem</button>';
    html += '<button onclick=\"submitFederationAdd()\" style=\"background:var(--primary);color:#fff;border:none;padding:8px 16px;border-radius:6px;cursor:pointer;font-weight:600\">Hozzáadás</button>';
    html += '</div></div>';
    
    body.innerHTML = html;
    document.getElementById('marveenModal').style.display = 'flex';
  };

  window.submitFederationAdd = function() {
    var token = localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || '';
    var data = {
      name: document.getElementById('fedPeerName').value,
      address: document.getElementById('fedPeerHost').value,
      port: parseInt(document.getElementById('fedPeerPort').value) || 8650,
      ssh_tunnel: document.getElementById('fedPeerSsh').checked
    };
    
    if (!data.name || !data.address) { alert('Kérjük, adjad meg a nevet és a hostot!'); return; }
    
    fetch('/api/federation/peer', {
      method: 'POST',
      headers: {'Content-Type': 'application/json', 'Authorization': 'Bearer ' + token},
      body: JSON.stringify(data)
    }).then(function(r) { return r.json(); })
      .then(function(d) {
        if (d.name) {
          closeMarveenModal();
          loadMarveenPage('federation');
        } else {
          alert('❌ Hiba: ' + (d.error || 'ismeretlen'));
        }
      }).catch(function(e) { alert('❌ Hálózati hiba: ' + e.message); });
  };

  window.showFederationDetail = function(name) {
    var token = localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || '';
    var body = document.getElementById('marveenModalBody');
    if (!body) return;
    var title = document.getElementById('marveenModalTitle');
    if (title) title.textContent = '🌐 ' + name + ' — Részletek';
    
    body.innerHTML = '<div style=\"text-align:center;padding:40px;color:var(--text3)\">Betöltés...</div>';
    document.getElementById('marveenModal').style.display = 'flex';
    
    // Fetch health and capabilities in parallel
    Promise.all([
      fetch('/api/federation/health/' + encodeURIComponent(name), { headers: { 'Authorization': 'Bearer ' + token } }).then(r => r.json()),
      fetch('/api/federation/capabilities/' + encodeURIComponent(name), { headers: { 'Authorization': 'Bearer ' + token } }).then(r => r.json())
    ]).then(function(results) {
      var health = results[0];
      var caps = results[1].capabilities || [];
      
      var html = '<div style=\"padding:16px\">';
      html += '<div style=\"margin-bottom:12px\"><strong style=\"color:var(--text)\">Státusz:</strong> ' + (health.status === 'online' ? '✅ online' : '❌ offline') + '</div>';
      html += '<div style=\"margin-bottom:12px\"><strong style=\"color:var(--text)\">Utolsó jel:</strong> ' + fmtTime(health.last_seen) + '</div>';
      html += '<div style=\"margin-bottom:12px\"><strong style=\"color:var(--text)\">Képességek:</strong></div>';
      if (caps.length) {
        html += '<div style=\"display:flex;gap:6px;flex-wrap:wrap;margin-top:8px;\">';
        caps.forEach(function(c) { html += badge(c, 'var(--primary)'); });
        html += '</div>';
      } else {
        html += '<div style=\"font-size:12px;color:var(--text3);margin-top:8px;\">Nincs lekérdezett képesség</div>';
      }
      html += '</div>';
      body.innerHTML = html;
    }).catch(function(e) {
      body.innerHTML = errorBox('Hiba a részletek lekérése közben: ' + e.message);
    });
  };

  var renderers = {
    kanban: function(d) {
      setTimeout(function() { window.initKanbanDnD && window.initKanbanDnD(); }, 100);
      if (!d || !d.boards) return empty('Nincs kanban adat');
      var boards = d.boards || [];
      if (!boards.length) return empty('Nincs kanban tábla');
      var html = '';
      boards.forEach(function(b) {
        var bid = esc(b.id || '');
        var cols = b.columns || ['todo','in_progress','review','done'];
        var cards = b.cards || [];
        var colLabels = {'todo':'📋 Teendő','in_progress':'🔄 Folyamatban','review':'👀 Felülvizsgálat','done':'✅ Kész'};
        var colColors = {'todo':'var(--text3)','in_progress':'var(--warning)','review':'var(--primary)','done':'var(--success)'};
        html += '<div style="margin-bottom:20px;">';
        html += '<div style="display:flex;align-items:center;gap:8px;margin-bottom:10px;">';
        html += '<h3 style="margin:0;font-size:15px;">' + esc(b.title || b.id) + '</h3>';
        html += badge(cards.length + ' kártya', 'var(--surface2)');
        html += '<button onclick="addKanbanCard(\'' + bid + '\')" style="background:var(--primary);color:#fff;border:none;padding:4px 12px;border-radius:6px;font-size:11px;cursor:pointer;margin-left:auto;">+ Kártya</button>';
        html += '</div>';
        // Column board
        html += '<div style="display:flex;gap:8px;overflow-x:auto;padding-bottom:8px;">';
        cols.forEach(function(col) {
          var colCards = cards.filter(function(c) { return (c.column || 'todo') === col; });
          var cl = colLabels[col] || col;
          var cc = colColors[col] || 'var(--text3)';
          html += '<div data-column="' + col + '" style="flex:1;min-width:180px;background:var(--surface);border-radius:10px;padding:8px;">';
          html += '<div style="font-size:11px;font-weight:600;color:' + cc + ';margin-bottom:8px;text-transform:uppercase;letter-spacing:0.5px;">' + esc(cl) + ' (' + colCards.length + ')</div>';
          colCards.forEach(function(c) {
            var cid = esc(c.id || '');
            var pri = c.priority || 'medium';
            var pc = pri === 'high' ? 'var(--danger)' : pri === 'low' ? 'var(--text3)' : 'var(--warning)';
            var approvable = c.requires_approval || c.approval_status === 'pending';
            window._kanbanCardsCache = window._kanbanCardsCache || {};
            window._kanbanCardsCache[bid + ':' + cid] = { card: c, boardId: bid, columns: cols, column: col };
            html += '<div data-card-id="' + cid + '" data-column="' + col + '" data-board-id="' + bid + '" draggable="true" ondragstart="handleDragStart(event)" ondragend="handleDragEnd(event)" onclick="showKanbanCardDetail(\'' + bid + '\',\'' + cid + '\')" style="background:var(--surface2);border:1px solid var(--border);border-radius:8px;padding:8px;margin-bottom:6px;cursor:pointer;">';
            html += '<div style="font-size:12px;font-weight:600;color:var(--text);margin-bottom:4px;">' + esc(c.title || '—') + '</div>';
            if (c.description) html += '<div style="font-size:10px;color:var(--text3);margin-bottom:4px;">' + esc(c.description.substring(0,80)) + '</div>';
            html += '<div style="display:flex;gap:4px;flex-wrap:wrap;align-items:center;">';
            html += '<span style="font-size:9px;padding:1px 6px;border-radius:4px;background:' + pc + ';color:#fff;font-weight:600;">' + esc(pri) + '</span>';
            if (c.assigned_to) html += '<span style="font-size:9px;color:var(--text2);">👤 ' + esc(c.assigned_to) + '</span>';
            html += '</div>';
            // Action buttons — gomboknál event.stopPropagation, hogy a kártya-kattintás (modal) ne nyíljon duplán
            html += '<div style="display:flex;gap:6px;margin-top:8px;">';
            var colIdx = cols.indexOf(col);
            if (colIdx > 0) html += '<button onclick="event.stopPropagation();moveKanbanCard(\'' + bid + '\',\'' + cid + '\',\'' + cols[colIdx-1] + '\')" style="background:var(--surface);border:1px solid var(--border);padding:6px 14px;border-radius:6px;font-size:14px;cursor:pointer;color:var(--text2);" title="Előző oszlopba">◀</button>';
            if (colIdx < cols.length-1) html += '<button onclick="event.stopPropagation();moveKanbanCard(\'' + bid + '\',\'' + cid + '\',\'' + cols[colIdx+1] + '\')" style="background:var(--surface);border:1px solid var(--border);padding:6px 14px;border-radius:6px;font-size:14px;cursor:pointer;color:var(--text2);" title="Következő oszlopba">▶</button>';
            if (approvable) html += '<button onclick="event.stopPropagation();approveKanbanCard(\'' + bid + '\',\'' + cid + '\')" style="background:var(--success);border:none;padding:6px 12px;border-radius:6px;font-size:12px;cursor:pointer;color:#fff;">✓</button>';
            html += '<button onclick="event.stopPropagation();deleteKanbanCard(\'' + bid + '\',\'' + cid + '\')" style="background:var(--surface);border:1px solid var(--danger);padding:6px 12px;border-radius:6px;font-size:12px;cursor:pointer;color:var(--danger);margin-left:auto;">✕</button>';
            html += '</div>';
            html += '</div>';
          });
          html += '</div>';
        });
        html += '</div>';
        html += '</div>';
      });
      return html;
    },

    approvals: function(d) {
      var html = '';
      var nodes = d.pending_nodes || [];
      var cards = d.pending_cards || [];
      if (!nodes.length && !cards.length) return empty('✅ Nincs függő jóváhagyás');
      if (nodes.length) {
        html += '<h3 style="margin:0 0 12px;font-size:15px;">🔗 Node Jóváhagyások (' + nodes.length + ')</h3>';
        html += table(['Node', 'Host', 'Port', 'Státusz'], nodes.map(function(n) {
          return [esc(n.node_name || n.name), esc(n.host), esc(n.port), badge(n.status || 'pending', 'var(--warning)')];
        }));
      }
      if (cards.length) {
        html += '<h3 style="margin:20px 0 12px;font-size:15px;">🎫 Kanban Jóváhagyások (' + cards.length + ')</h3>';
        html += table(['Cím', 'Assignee', 'Prioritás', 'Létrehozva'], cards.map(function(c) {
          return [esc(c.title), esc(c.assignee || '—'), badge(c.priority || 'normal'), fmtTime(c.created_at)];
        }));
      }
      return html;
    },

    activity: function(d) {
      var acts = d.activities || [];
      if (!acts.length) return empty('Nincs aktivitás');
      // Summary by type
      var typeCounts = {};
      acts.forEach(function(a) {
        var t = a.msg_type || 'other';
        typeCounts[t] = (typeCounts[t] || 0) + 1;
      });
      var topTypes = Object.keys(typeCounts).sort(function(a, b) { return typeCounts[b] - typeCounts[a]; }).slice(0, 5);
      var html = '<div style="display:flex;gap:6px;margin-bottom:12px;flex-wrap:wrap;">';
      topTypes.forEach(function(t) {
        html += '<span style="background:var(--surface2);padding:4px 10px;border-radius:8px;font-size:10px;border:1px solid var(--border);">' + esc(t) + ': <strong>' + esc(String(typeCounts[t])) + '</strong></span>';
      });
      html += '</div>';
      // Filter input
      html += '<input type="text" id="activityFilter" placeholder="🔍 Szűrés feladó/típus..." onkeyup="filterActivityTable()" style="width:100%;padding:8px 12px;border:1px solid var(--border);border-radius:8px;background:var(--surface);color:var(--text);font-size:12px;margin-bottom:8px;box-sizing:border-box;">';
      html += '<div id="activityTableWrap">';
      html += table(['Feladó', 'Címzett', 'Típus', 'Prioritás', 'Idő'], acts.slice(0, 50).map(function(a) {
        var pri = a.priority || 'normal';
        var pc = pri === 'high' ? 'var(--danger)' : pri === 'low' ? 'var(--text3)' : 'var(--primary)';
        return [esc(a.sender), esc(a.recipient), badge(a.msg_type || 'msg'), badge(pri, pc), fmtTime(a.created_at)];
      }));
      html += '</div>';
      if (acts.length > 50) {
        html += '<div style="text-align:center;color:var(--text3);font-size:11px;margin-top:8px;">+' + (acts.length - 50) + ' további aktivitás</div>';
      }
      return html;
    },

    bgTasks: function(d) {
      var jobs = d.jobs || d.tasks || d.cron_jobs || [];
      if (d.error) return errorBox(d.error);
      var html = '<div style="margin-bottom:12px;"><button onclick="showCronAddModal()" style="background:var(--primary);color:#fff;border:none;padding:6px 12px;border-radius:6px;cursor:pointer;font-size:11px;">➕ Új cron task</button></div>';
      if (!jobs.length) return html + empty('Nincs háttér feladat');
      return html + table(['Név', 'Ütemezés', 'Státusz', 'Utolsó futás'], jobs.map(function(j) {
        return [esc(j.name || j.id), esc(j.schedule || j.cron || '—'), badge(j.status || 'active'), fmtTime(j.last_run || j.last_execution)];
      }));
    },

    memories: function(d) {
      if (d.error) return errorBox(d.error);
      var cats = d.categories || {};
      var total = d.total || 0;
      // Vector search bar
      var html = '<div style="margin-bottom:16px;background:var(--surface2);border:1px solid var(--border);border-radius:12px;padding:16px;">';
      html += '<div style="font-size:14px;font-weight:600;color:var(--text);margin-bottom:8px;">🔍 Vektor keresés a memóriában</div>';
      html += '<div style="display:flex;gap:8px;">';
      html += '<input id="memorySearchInput" type="text" placeholder="Keresés a delegation eredmények és tudásbázis között..." style="flex:1;background:var(--bg);border:1px solid var(--border);color:var(--text);padding:10px 14px;border-radius:8px;font-size:14px;" onkeyup="if(event.key===\'Enter\')memoryVectorSearch()" />';
      html += '<button onclick="memoryVectorSearch()" style="background:var(--primary);color:#fff;border:none;padding:10px 20px;border-radius:8px;cursor:pointer;font-size:14px;">🔍 Keres</button>';
      html += '</div>';
      html += '<div id="memorySearchResults" style="margin-top:12px;"></div>';
      html += '</div>';
      if (!total) return html + empty('Nincs memória adat');
      // Category labels (Hungarian)
      var catLabels = {
        'directive': '📋 Utasítások',
        'a2a_message': '💬 Agent üzenetek',
        'agent_reply': '↩️ Válaszok',
        'memory': '🧠 Memóriák',
        'task': '✅ Feladatok',
        'context': '📄 Kontextus',
        'other': '📦 Egyéb'
      };
      html += '<div style="margin-bottom:12px;display:flex;gap:8px;flex-wrap:wrap;">';
      html += '<span style="background:var(--primary);color:#fff;padding:4px 12px;border-radius:8px;font-size:12px;font-weight:600;">Összes: ' + total + '</span>';
      Object.keys(cats).forEach(function(c) {
        var label = catLabels[c] || ('📦 ' + c);
        html += '<span style="background:var(--surface2);border:1px solid var(--border);padding:4px 12px;border-radius:8px;font-size:12px;">' + label + ' (' + cats[c].count + ')</span>';
      });
      html += '</div>';
      // Render each category as a section with cards
      Object.keys(cats).forEach(function(c) {
        var label = catLabels[c] || ('📦 ' + c);
        var items = cats[c].items || [];
        html += '<div style="margin-bottom:16px;">';
        html += '<div style="font-size:14px;font-weight:600;color:var(--text);margin-bottom:8px;padding-bottom:4px;border-bottom:2px solid var(--border);">' + label + ' (' + items.length + ')</div>';
        items.forEach(function(m) {
          var subj = esc(m.subject || '(nincs tárgy)');
          var content = esc((m.content || '').substring(0, 300));
          var sender = esc(m.sender_agent || '?');
          var recip = esc(m.recipient_agent || '?');
          var prio = m.priority || 5;
          var prioColor = prio >= 7 ? 'var(--danger)' : prio >= 4 ? 'var(--warning)' : 'var(--text3)';
          var status = esc(m.status || '—');
          var time = fmtTime(m.created_at);
          var mid = esc(m.id || '');
          html += '<div onclick="showMemoryDetail(\'' + mid + '\')" style="background:var(--surface2);border:1px solid var(--border);border-radius:10px;padding:12px;margin-bottom:8px;cursor:pointer;transition:border-color 0.2s;" onmouseover="this.style.borderColor=\'var(--primary)\'" onmouseout="this.style.borderColor=\'var(--border)\'">';
          html += '<div style="display:flex;align-items:center;gap:8px;margin-bottom:6px;">';
          html += '<strong style="font-size:13px;color:var(--text);">' + subj + '</strong>';
          html += '<span style="margin-left:auto;font-size:10px;padding:2px 6px;border-radius:4px;background:' + prioColor + ';color:#fff;font-weight:600;">P' + prio + '</span>';
          if (status === 'read') html += '<span style="font-size:10px;color:var(--success);">✓ olvasott</span>';
          html += '</div>';
          html += '<div style="font-size:12px;color:var(--text3);line-height:1.5;max-height:60px;overflow:hidden;">' + content + '</div>';
          html += '<div style="display:flex;gap:12px;margin-top:6px;font-size:11px;color:var(--text3);">';
          html += '<span>📤 ' + sender + '</span><span>📥 ' + recip + '</span><span style="margin-left:auto;">' + time + '</span>';
          html += '</div></div>';
        });
        html += '</div>';
      });
      return html;
    },

    naplo: function(d) {
      if (d.error) return errorBox(d.error);
      var logs = d.logs || [];
      var filters = d.filters || {};
      var total = d.total || 0;
      // Filter bar
      var html = '<div style="margin-bottom:12px;">';
      // Type filter buttons
      html += '<div style="display:flex;gap:6px;flex-wrap:wrap;margin-bottom:8px;">';
      var typeAll = (!filters.type || filters.type === 'all');
      html += '<span onclick="filterLogs(\'all\')" style="background:' + (typeAll ? 'var(--primary)' : 'var(--surface2)') + ';color:' + (typeAll ? '#fff' : 'var(--text2)') + ';border:1px solid var(--border);padding:4px 12px;border-radius:8px;font-size:12px;cursor:pointer;font-weight:600;">Összes (' + total + ')</span>';
      html += '<span onclick="filterLogs(\'delegation\')" style="background:var(--surface2);border:1px solid var(--border);padding:4px 12px;border-radius:8px;font-size:12px;cursor:pointer;">📋 Delegációk</span>';
      html += '<span onclick="filterLogs(\'health\')" style="background:var(--surface2);border:1px solid var(--border);padding:4px 12px;border-radius:8px;font-size:12px;cursor:pointer;">💊 Health</span>';
      html += '</div>';
      // Search + node filter row
      html += '<div style="display:flex;gap:8px;flex-wrap:wrap;margin-bottom:8px;">';
      html += '<input id="logSearchInput" type="text" placeholder="🔍 Keresés tárgyban..." value="' + esc(filters.q || '') + '" onkeyup="if(event.key===\'Enter\')applyLogFilters()" style="flex:1;min-width:150px;padding:6px 10px;border-radius:8px;border:1px solid var(--border);background:var(--surface2);color:var(--text);font-size:12px;" />';
      html += '<select id="logNodeFilter" onchange="applyLogFilters()" style="padding:6px 10px;border-radius:8px;border:1px solid var(--border);background:var(--surface2);color:var(--text);font-size:12px;">';
      html += '<option value="">All Nodes</option>';
      var nodes = ['nova','morzsa','runa','tor'];
      for (var i = 0; i < nodes.length; i++) {
        html += '<option value="' + nodes[i] + '"' + (filters.node === nodes[i] ? ' selected' : '') + '>' + nodes[i] + '</option>';
      }
      html += '</select>';
      html += '<select id="logStatusFilter" onchange="applyLogFilters()" style="padding:6px 10px;border-radius:8px;border:1px solid var(--border);background:var(--surface2);color:var(--text);font-size:12px;">';
      html += '<option value="">All Status</option>';
      var statuses = ['completed','failed','available','pending','in_progress'];
      for (var j = 0; j < statuses.length; j++) {
        html += '<option value="' + statuses[j] + '"' + (filters.status === statuses[j] ? ' selected' : '') + '>' + statuses[j] + '</option>';
      }
      html += '</select>';
      // Date range
      html += '<input id="logDateFrom" type="date" value="' + esc(filters.from || '') + '" onchange="applyLogFilters()" style="padding:6px 10px;border-radius:8px;border:1px solid var(--border);background:var(--surface2);color:var(--text);font-size:12px;" />';
      html += '<input id="logDateTo" type="date" value="' + esc(filters.to || '') + '" onchange="applyLogFilters()" style="padding:6px 10px;border-radius:8px;border:1px solid var(--border);background:var(--surface2);color:var(--text);font-size:12px;" />';
      html += '</div>';
      // Action buttons
      html += '<div style="display:flex;gap:8px;margin-bottom:8px;">';
      html += '<button onclick="applyLogFilters()" style="background:var(--primary);color:#fff;border:none;padding:6px 16px;border-radius:8px;font-size:12px;cursor:pointer;font-weight:600;">Szűrés</button>';
      html += '<button onclick="clearLogFilters()" style="background:var(--surface2);color:var(--text2);border:1px solid var(--border);padding:6px 16px;border-radius:8px;font-size:12px;cursor:pointer;">Törlés</button>';
      html += '<button onclick="exportLogs(\'csv\')" style="background:var(--surface2);color:var(--text2);border:1px solid var(--border);padding:6px 16px;border-radius:8px;font-size:12px;cursor:pointer;margin-left:auto;">⬇ CSV</button>';
      html += '<button onclick="exportLogs(\'json\')" style="background:var(--surface2);color:var(--text2);border:1px solid var(--border);padding:6px 16px;border-radius:8px;font-size:12px;cursor:pointer;">⬇ JSON</button>';
      html += '</div>';
      html += '</div>';
      if (!logs.length) return html + empty('Nincs napló bejegyzés a szűrőfeltételeknek megfelelően');
      // Log entries
      logs.forEach(function(l) {
        var lt = l.log_type || 'delegation';
        var typeIcon = lt === 'health' ? '💊' : '📋';
        var status = l.status || '—';
        var statusColor = status === 'completed' ? 'var(--success)' : status === 'failed' ? 'var(--danger)' : status === 'available' ? 'var(--warning)' : 'var(--text3)';
        var time = fmtTime(l.created_at || l.updated_at || l.last_seen);
        var subj = esc(l.subject || l.node_name || '(nincs)');
        var fromA = esc(l.from_agent || '');
        var toA = esc(l.to_agent || l.assigned_agent || '');
        var lid = esc(l.id || l.task_id || '');
        var detailJson = JSON.stringify(l).replace(/'/g, "\\'").replace(/"/g, '&quot;');
        html += '<div onclick="showLogDetail(\'' + lid + '\', \'' + detailJson + '\')" style="background:var(--surface2);border:1px solid var(--border);border-radius:10px;padding:12px;margin-bottom:6px;cursor:pointer;transition:border-color 0.2s;" onmouseover="this.style.borderColor=\'var(--primary)\'" onmouseout="this.style.borderColor=\'var(--border)\'">';
        html += '<div style="display:flex;align-items:center;gap:8px;">';
        html += '<span style="font-size:16px;">' + typeIcon + '</span>';
        html += '<strong style="font-size:13px;color:var(--text);flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">' + subj + '</strong>';
        html += '<span style="font-size:10px;padding:2px 8px;border-radius:4px;background:' + statusColor + ';color:#fff;font-weight:600;text-transform:uppercase;">' + esc(status) + '</span>';
        html += '</div>';
        html += '<div style="display:flex;gap:12px;margin-top:6px;font-size:11px;color:var(--text3);">';
        if (fromA) html += '<span>📤 ' + fromA + '</span>';
        if (toA) html += '<span>📥 ' + toA + '</span>';
        if (l.priority) html += '<span>P' + l.priority + '</span>';
        html += '<span style="margin-left:auto;">' + time + '</span>';
        html += '</div></div>';
      });
      return html;
    },

    research: function(d) {
      // Ötletláda — Kanban-style idea board
      var ideas = d.ideas || [];
      var stats = d.stats || {};
      if (d.error) return errorBox(d.error);
      if (!ideas.length && !d.error) {
        return '<div style="text-align:center;padding:40px;color:var(--text3);">' +
          '<div style="font-size:48px;margin-bottom:16px;">💡</div>' +
          '<div style="font-size:16px;margin-bottom:8px;">Nincsenek ötletek még</div>' +
          '<div style="font-size:13px;margin-bottom:20px;">Légy te az első, aki javasol valamit!</div>' +
          '<button onclick="showIdeaSubmitForm()" style="background:var(--primary);color:#fff;border:none;padding:10px 24px;border-radius:8px;cursor:pointer;font-size:14px;">+ Új ötlet</button>' +
          '</div>';
      }

      // Stats bar
      var html = '<div style="display:flex;gap:8px;margin-bottom:16px;flex-wrap:wrap;">';
      var statItems = [
        {label: 'Összes', val: stats.total || 0, color: 'var(--primary)'},
        {label: '💡 Ötlet', val: stats.idea || 0, color: '#60a5fa'},
        {label: '✅ Elfogadott', val: stats.approved || 0, color: '#4ade80'},
        {label: '🔄 Folyamatban', val: stats.in_progress || 0, color: '#fbbf24'},
        {label: '✔️ Kész', val: stats.done || 0, color: '#22c55e'},
        {label: '❌ Elutasított', val: stats.rejected || 0, color: '#ef4444'}
      ];
      statItems.forEach(function(s) {
        html += '<div style="background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:8px 14px;text-align:center;min-width:80px;">' +
          '<div style="font-size:20px;font-weight:700;color:' + s.color + ';">' + esc(s.val) + '</div>' +
          '<div style="font-size:10px;color:var(--text3);margin-top:2px;">' + esc(s.label) + '</div></div>';
      });
      html += '</div>';

      // Submit button
      html += '<div style="display:flex;gap:8px;margin-bottom:16px;">';
      html += '<button onclick="showIdeaSubmitForm()" style="background:var(--primary);color:#fff;border:none;padding:8px 20px;border-radius:8px;cursor:pointer;font-size:13px;">+ Új ötlet beküldése</button>';
      html += '<button onclick="importDiagnosticsAsIdeas()" style="background:var(--surface2);color:var(--text2);border:1px solid var(--border);padding:8px 20px;border-radius:8px;cursor:pointer;font-size:13px;">🔧 Diagnosztika importálása</button>';
      html += '</div>';

      // Kanban columns
      var columns = [
        {id: 'idea', title: '💡 Ötletek', color: '#60a5fa'},
        {id: 'approved', title: '✅ Elfogadott', color: '#4ade80'},
        {id: 'in_progress', title: '🔄 Folyamatban', color: '#fbbf24'},
        {id: 'done', title: '✔️ Kész', color: '#22c55e'},
        {id: 'rejected', title: '❌ Elutasított', color: '#ef4444'}
      ];
      // Kattintható kártyák + idea cache a részletes nézethez
      window._ideasCache = {};
      ideas.forEach(function(i) { window._ideasCache[i.id] = i; });
      // Sidebar számláló frissítés
      var openCount = ideas.filter(function(i) { return i.status === 'idea'; }).length;
      var cnt = document.getElementById('idea-counter');
      if (cnt) { cnt.textContent = openCount; cnt.style.display = openCount > 0 ? 'inline-block' : 'none'; }

      html += '<div style="display:flex;gap:12px;overflow-x:auto;padding-bottom:8px;">';
      columns.forEach(function(col) {
        var colIdeas = ideas.filter(function(i) { return i.status === col.id; });
        html += '<div style="flex:1;min-width:200px;max-width:300px;">' +
          '<div style="font-size:13px;font-weight:600;margin-bottom:8px;color:' + col.color + ';">' + esc(col.title) + ' (' + colIdeas.length + ')</div>' +
          '<div style="display:flex;flex-direction:column;gap:8px;">';
        colIdeas.forEach(function(idea) {
          var priColor = idea.priority === 'high' ? 'var(--danger)' : idea.priority === 'low' ? 'var(--text3)' : 'var(--primary)';
          var sourceBadge = idea.source_type === 'agent' ? '<span style="font-size:9px;background:#3b1f5f;color:#c084fc;padding:1px 6px;border-radius:8px;">🤖</span>' : '';
          var integratedBadge = idea.integrated ? '<span style="font-size:9px;background:#14532d;color:#4ade80;padding:1px 6px;border-radius:8px;font-weight:600;" title="Beépült a repóba: ' + esc(idea.integrated_file || '') + '">📦 beépítve</span>' : '';
          // Idő-alapú továbblépés jelzése: hátralévő órák a 48h/72h küszöbig
          var ageBadge = '';
          if (idea.status === 'idea' && idea.created_at) {
            var ageH = Math.floor((Date.now() - new Date(idea.created_at).getTime()) / 3600000);
            var remaining = 48 - ageH;
            if (idea.score >= 1) {
              ageBadge = remaining > 0
                ? '<span style="font-size:9px;background:#1e3a5f;color:#60a5fa;padding:1px 6px;border-radius:8px;" title="48h után score ≥ +1 esetén automatikus elfogadás">⏳ ' + remaining + 'h → auto</span>'
                : '<span style="font-size:9px;background:#14532d;color:#4ade80;padding:1px 6px;border-radius:8px;">⏳ küszöb elérve</span>';
            } else if (idea.score === 0) {
              var remainingR = 48 - ageH;
              ageBadge = remainingR > 0
                ? '<span style="font-size:9px;background:#3b2f5f;color:#a78bfa;padding:1px 6px;border-radius:8px;" title="48h után 0 score-nál koordinátor-review dönt">⏳ ' + remainingR + 'h → review</span>'
                : '<span style="font-size:9px;background:#3b2f5f;color:#c084fc;padding:1px 6px;border-radius:8px;">⏳ review dönt</span>';
            } else {
              var remainingRej = 72 - ageH;
              ageBadge = remainingRej > 0
                ? '<span style="font-size:9px;background:#5f1e1e;color:#f87171;padding:1px 6px;border-radius:8px;" title="72h után negatív score-nál automatikus elutasítás">⏳ ' + remainingRej + 'h →reject</span>'
                : '<span style="font-size:9px;background:#5f1e1e;color:#ef4444;padding:1px 6px;border-radius:8px;">⏳ reject küszöb</span>';
            }
          }
          html += '<div onclick="showIdeaDetail(\'' + esc(idea.id) + '\')" style="background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:10px;cursor:pointer;transition:border-color .15s;" onmouseover="this.style.borderColor=\'var(--primary)\'" onmouseout="this.style.borderColor=\'var(--border)\'" id="idea-' + esc(idea.id) + '">' +
            '<div style="display:flex;align-items:flex-start;gap:6px;margin-bottom:6px;">' +
            '<strong style="font-size:13px;flex:1;">' + esc(idea.title) + '</strong>' +
            '<span style="font-size:9px;background:' + priColor + '22;color:' + priColor + ';padding:1px 6px;border-radius:8px;flex-shrink:0;">' + esc(idea.priority) + '</span>' +
            '</div>';
          if (idea.description) {
            html += '<div style="font-size:11px;color:var(--text3);line-height:1.4;margin-bottom:6px;">' + esc(idea.description.substring(0, 90)) + (idea.description.length > 90 ? '…' : '') + '</div>';
          }
          html += '<div style="display:flex;align-items:center;gap:8px;margin-top:8px;padding-top:8px;border-top:1px solid var(--border);font-size:11px;">' +
            '<span style="color:#4ade80;">👍 ' + esc(idea.upvotes) + '</span>' +
            '<span style="color:#ef4444;">👎 ' + esc(idea.downvotes) + '</span>' +
            '<span style="font-weight:600;color:' + (idea.score > 0 ? '#4ade80' : idea.score < 0 ? '#ef4444' : 'var(--text3)') + ';">' + (idea.score > 0 ? '+' : '') + esc(idea.score) + '</span>' +
            '<span style="margin-left:auto;color:var(--text3);">' + ageBadge + ' ' + integratedBadge + ' ' + sourceBadge + ' ' + esc(idea.submitted_by) + '</span>' +
            '</div>' +
            '</div>';
        });
        if (!colIdeas.length) {
          html += '<div style="text-align:center;padding:20px;color:var(--text3);font-size:11px;">Üres</div>';
        }
        html += '</div></div>';
      });
      html += '</div>';
      return html;
    },

    costs: function(d) {
      if (d.error) return errorBox(d.error);
      var html = '';
      html += '<div style="display:flex;gap:8px;margin-bottom:12px;"><button onclick="showBudgetModal()" style="background:var(--primary);color:#fff;border:none;padding:6px 12px;border-radius:6px;cursor:pointer;font-size:11px;">💰 Budget beállítás</button></div>';
      var total = d.total_cost || d.total || 0;
      var html = card('<div style="text-align:center;padding:10px;">' +
        '<div style="font-size:28px;font-weight:700;color:var(--primary);">$' + esc(total.toFixed ? total.toFixed(2) : total) + '</div>' +
        '<div style="color:var(--text3);font-size:12px;margin-top:4px;">Összes költség</div></div>');
      var entries = d.entries || d.costs || d.breakdown || [];
      if (entries.length) {
        html += table(['Tétel', 'Költség', 'Dátum'], entries.map(function(e) {
          return [esc(e.name || e.category || e.description || '—'), '$' + esc(e.cost || e.amount || 0), fmtTime(e.date || e.created_at)];
        }));
      }
      return html;
    },

    tokenUsage: function(d) {
      if (d.error) return errorBox(d.error);
      var total = d.total_tokens || d.total || 0;
      var html = '<canvas id="tokenChartCanvas" style="width:100%;height:200px;margin-top:12px;border-radius:8px;"></canvas>';
      html += '<div style="text-align:center;padding:10px;background:var(--surface2);border-radius:8px;margin:8px 0;">';
      html += '<div style="font-size:28px;font-weight:700;color:var(--primary);">' + esc(String(total)) + '</div>';
      html += '<div style="color:var(--text3);font-size:12px;margin-top:4px;">Összes token</div></div>';
      var byAgent = d.by_agent || {};
      var agentKeys = Object.keys(byAgent);
      if (agentKeys.length) {
        html += table(['Agent', 'Input', 'Output', 'Requests'], agentKeys.map(function(name) {
          var a = byAgent[name] || {};
          return [esc(name), esc(String(a.input || 0)), esc(String(a.output || 0)), esc(String(a.requests || 0))];
        }));
      }
      setTimeout(function() { window.loadTokenChart && window.loadTokenChart(); }, 50);
      return html;
    },

    updates: function(d) {
      if (d.error) return errorBox(d.error);
      var upToDate = d.up_to_date;
      var pendingCount = d.pending_count || 0;
      var pendingCommits = d.pending_commits || [];
      var recentCommits = d.recent_commits || [];
      var branch = d.branch || 'main';
      var localHead = d.local_head || '?';
      var remoteHead = d.remote_head || '?';
      var updateAvail = d.update_available;
      var html = '';
      // Status card
      var statusColor = upToDate ? 'var(--success)' : 'var(--warning)';
      var statusIcon = upToDate ? '✅' : '⬆️';
      var statusText = upToDate ? 'Naprakész' : (pendingCount + ' frissítés érhető el');
      html += '<div style="background:var(--surface2);border-radius:8px;padding:14px;margin-bottom:16px;border-left:4px solid ' + statusColor + ';">';
      html += '<div style="display:flex;align-items:center;gap:8px;margin-bottom:8px;">';
      html += '<span style="font-size:20px;">' + statusIcon + '</span>';
      html += '<strong style="font-size:14px;color:' + statusColor + ';">' + esc(statusText) + '</strong>';
      html += '</div>';
      html += '<div style="display:flex;gap:16px;font-size:11px;color:var(--text3);">';
      html += '<span>Branch: <strong>' + esc(branch) + '</strong></span>';
      html += '<span>Local: <code>' + esc(localHead) + '</code></span>';
      html += '<span>Remote: <code>' + esc(remoteHead) + '</code></span>';
      html += '</div>';
      html += '</div>';
      // Action buttons
      html += '<div style="display:flex;gap:8px;margin-bottom:16px;">';
      if (updateAvail) {
        html += '<button onclick="meshUpdatePull()" style="background:var(--primary);color:#fff;border:none;padding:8px 16px;border-radius:8px;cursor:pointer;font-size:12px;">⬇️ Pull + Deploy</button>';
        html += '<button onclick="meshUpdateCheck()" style="background:var(--surface2);color:var(--text);border:1px solid var(--border);padding:8px 16px;border-radius:8px;cursor:pointer;font-size:12px;">🔄 Ellenőriz</button>';
      } else {
        html += '<button onclick="meshUpdateCheck()" style="background:var(--surface2);color:var(--text);border:1px solid var(--border);padding:8px 16px;border-radius:8px;cursor:pointer;font-size:12px;">🔄 Ellenőriz</button>';
        html += '<button onclick="meshDeployAll()" style="background:var(--warning);color:#000;border:none;padding:8px 16px;border-radius:8px;cursor:pointer;font-size:12px;">🚀 Deploy minden node-ra</button>';
      }
      html += '</div>';
      // Pending commits
      if (pendingCommits.length) {
        html += '<h3 style="margin:0 0 8px;font-size:13px;color:var(--warning);">⬆️ Várakozó frissítések (' + pendingCommits.length + ')</h3>';
        pendingCommits.forEach(function(c) {
          html += card('<div style="font-size:11px;font-family:monospace;color:var(--text2);">' + esc(c) + '</div>');
        });
      }
      // Recent commits
      if (recentCommits.length) {
        html += '<h3 style="margin:16px 0 8px;font-size:13px;color:var(--text3);">📋 Legutóbbi commitok</h3>';
        recentCommits.forEach(function(c) {
          html += card('<div style="font-size:11px;font-family:monospace;color:var(--text2);">' + esc(c) + '</div>');
        });
      }
      return html;
    },

    vault: function(d) {
      if (d.error) return errorBox(d.error);
      var items = d.items || d.secrets || d.entries || [];
      if (!Array.isArray(items) && typeof items === 'object') items = Object.keys(items).map(function(k) { return items[k] || {}; });
      var html = '';
      // ── Agent-váltó: melyik node vaultját nézzük ──
      html += '<div id="vault-agent-bar" style="display:flex;gap:8px;margin-bottom:14px;flex-wrap:wrap;align-items:center;"></div>';
      html += '<div style="display:flex;gap:8px;margin-bottom:16px;flex-wrap:wrap;">';
      html += '<button onclick="vaultAddEntry(window._vaultCurrentAgent)" style="background:var(--primary);color:#fff;border:none;padding:8px 16px;border-radius:8px;cursor:pointer;font-size:12px;">➕ Tétel hozzáadása</button>';
      html += '<button onclick="vaultShowMeshOverview()" style="background:var(--surface2);color:var(--text);border:1px solid var(--border);padding:8px 16px;border-radius:8px;cursor:pointer;font-size:12px;">🌐 Mesh áttekintés</button>';
      html += '<button onclick="window._vaultRefresh()" title="Frissítés" style="background:var(--surface2);color:var(--text);border:1px solid var(--border);padding:8px 12px;border-radius:8px;cursor:pointer;font-size:12px;">🔄</button>';
      html += '</div>';
      html += '<div id="vault-entries"></div>';
      html += '<div id="vault-mesh-section" style="margin-top:12px;"></div>';
      return html;
    },

    vaultMesh: function(d) {
      // /api/vault/mesh válasz — minden node vault státusza
      var html = '<h3 style="margin:0 0 10px;font-size:13px;color:var(--text2);">🔐 Vault minden node-on</h3>';
      var nodes = d.nodes || {};
      html += '<div style="display:flex;flex-direction:column;gap:8px;">';
      Object.keys(nodes).forEach(function(name) {
        var nd = nodes[name] || {};
        var vs = nd.vault_status || {};
        var backend = vs.backend || '?';
        var count = vs.entry_count != null ? vs.entry_count : (vs.entries || []).length;
        var healthy = vs.initialized || nd.local;
        html += card('<div style="display:flex;align-items:center;gap:10px;">' +
          '<div style="width:36px;height:36px;border-radius:50%;background:' + (healthy ? 'var(--success)' : 'var(--warning)') + ';display:flex;align-items:center;justify-content:center;font-size:16px;">🔐</div>' +
          '<div style="flex:1;min-width:0;">' +
            '<div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap;"><strong style="font-size:13px;">' + esc(name) + '</strong>' +
            (nd.local ? badge('helyi', 'var(--primary)') : badge('távoli', '#c084fc')) +
            '</div>' +
            '<div style="font-size:10px;color:var(--text3);margin-top:2px;">Bejegyzések: <strong style="color:var(--text);">' + esc(count) + '</strong> • backend: ' + esc(String(backend).substring(0, 60)) + '</div>' +
          '</div>' +
          '<button onclick="vaultSelectAgent(\'' + esc(name) + '\')" style="font-size:11px;background:var(--surface2);color:var(--text);border:1px solid var(--border);padding:6px 10px;border-radius:6px;cursor:pointer;">📂 Megnyitás</button>' +
        '</div>');
      });
      html += '</div>';
      return html;
    },

    connectors: function(d) {
      // MCP Registry — Marveen-inspired node-grouped card layout
      if (d.error) return errorBox(d.error);
      var nodes = d.nodes || [];
      var totalServers = d.total_servers || 0;
      if (!nodes.length) return empty('Nincs MCP adat');

      // Summary bar
      var html = '<div style="display:flex;gap:12px;margin-bottom:16px;flex-wrap:wrap">' +
        '<div style="background:var(--surface);border-radius:8px;padding:12px 16px;text-align:center;flex:1;min-width:120px">' +
        '<div style="font-size:28px;font-weight:700;color:var(--primary)">' + nodes.length + '</div>' +
        '<div style="font-size:11px;color:var(--text3)">Node-ok</div></div>' +
        '<div style="background:var(--surface);border-radius:8px;padding:12px 16px;text-align:center;flex:1;min-width:120px">' +
        '<div style="font-size:28px;font-weight:700;color:var(--success)">' + totalServers + '</div>' +
        '<div style="font-size:11px;color:var(--text3)">MCP szerverek</div></div>' +
        '</div>';

      // Group by node
      nodes.forEach(function(node) {
        var nodeName = esc(node.node || 'unknown');
        var nodeStatus = node.status || 'unknown';
        var statusIcon = nodeStatus === 'local' ? '🏠' : '🔗';
        var statusColor = nodeStatus === 'local' ? 'var(--primary)' : 'var(--info)';
        var servers = node.mcp_servers || [];

        html += '<div style="margin-bottom:16px">' +
          '<div style="display:flex;align-items:center;gap:8px;margin-bottom:8px;padding:8px 12px;background:var(--surface);border-radius:8px;border-left:3px solid ' + statusColor + '">' +
          '<span style="font-size:16px">' + statusIcon + '</span>' +
          '<strong style="color:var(--text)">' + nodeName + '</strong>' +
          '<span style="font-size:11px;color:var(--text3)">' + esc(node.host || '') + '</span>' +
          '<span style="margin-left:auto;font-size:11px;color:var(--text3)">' + servers.length + ' MCP</span>' +
          '</div>';

        if (!servers.length) {
          html += '<div style="padding:8px 16px;color:var(--text3);font-size:12px">Nincs MCP szerver</div>';
        } else {
          // MCP server cards
          html += '<div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(240px,1fr));gap:8px;padding:0 4px">';
          servers.forEach(function(s) {
            if (s.error) {
              html += '<div style="background:var(--surface);border-radius:8px;padding:12px;border:1px solid var(--danger)">' +
                '<div style="color:var(--danger);font-size:12px">⚠️ ' + esc(s.error) + '</div></div>';
              return;
            }
            var name = esc(s.name || 'unknown');
            var enabled = s.enabled !== false;
            var transport = s.transport || 'stdio';
            var url = s.url || '';
            var command = s.command || '';
            var hasCreds = s.has_credentials;
            var isLocal = nodeStatus === 'local';
            var alreadyInstalled = isLocal && enabled;

            // Transport icon
            var transIcon = transport === 'streamable_http' ? '🌐' : '📦';
            var transLabel = transport === 'streamable_http' ? 'HTTP' : 'STDIO';

            // Status badge
            var stBadge = enabled ?
              '<span style="background:var(--success);color:#fff;padding:2px 6px;border-radius:4px;font-size:10px">aktív</span>' :
              '<span style="background:var(--text3);color:#fff;padding:2px 6px;border-radius:4px;font-size:10px">inaktív</span>';

            // Credential badge
            var credBadge = hasCreds ?
              '<span style="font-size:10px;color:var(--warning)" title="Környezeti változókat igényel">🔐 cred</span>' : '';

            // Install button (only for remote servers not yet installed locally)
            var installBtn = '';
            if (!isLocal) {
              // Check if this is a remote HTTP server that can be used locally
              if (url) {
                installBtn = '<button onclick="installMcpServer(\'' + esc(s.name) + '\', \'' + esc(url) + '\', \'' + esc(nodeName) + '\')" ' +
                  'style="background:var(--primary);color:#fff;border:none;padding:4px 10px;border-radius:4px;cursor:pointer;font-size:11px;margin-top:6px">🔌 Beépítem</button>';
              } else if (command) {
                installBtn = '<button onclick="installMcpServer(\'' + esc(s.name) + '\', \'\', \'' + esc(nodeName) + '\', \'' + esc(command) + '\', ' + JSON.stringify(s.args || []).replace(/'/g, "&#39;") + ')" ' +
                  'style="background:var(--info);color:#fff;border:none;padding:4px 10px;border-radius:4px;cursor:pointer;font-size:11px;margin-top:6px">📋 Másolom</button>';
              }
            }

            var cardContent = '<div style="font-weight:600;font-size:13px;margin-bottom:4px">' + transIcon + ' ' + name + '</div>' +
              '<div style="display:flex;gap:6px;align-items:center;margin-bottom:4px">' + stBadge + ' ' + credBadge + '</div>' +
              '<div style="font-size:11px;color:var(--text3);margin-bottom:2px"><strong>' + transLabel + '</strong></div>';
            if (url) cardContent += '<div style="font-size:10px;color:var(--text3);word-break:break-all">🔗 ' + esc(url) + '</div>';
            if (command) cardContent += '<div style="font-size:10px;color:var(--text3)">$ ' + esc(command) + ' ' + esc(Array.isArray(s.args) ? s.args.join(' ') : (typeof s.args === 'string' ? s.args : '')) + '</div>';
            if (s.env_keys && s.env_keys.length) cardContent += '<div style="font-size:10px;color:var(--warning);margin-top:2px">Env: ' + esc(s.env_keys.join(', ')) + '</div>';
            cardContent += installBtn;

            html += '<div onclick="showMcpDetail(\'' + esc(s.name) + '\', \'' + esc(nodeName) + '\', ' + JSON.stringify(s).replace(/'/g, "&#39;").replace(/"/g, '&quot;') + ')" ' +
              'style="background:var(--surface);border-radius:8px;padding:12px;cursor:pointer;border:1px solid var(--surface2);transition:border-color 0.2s" ' +
              'onmouseover="this.style.borderColor=\'var(--primary)\'" onmouseout="this.style.borderColor=\'var(--surface2)\'">' +
              cardContent + '</div>';
          });
          html += '</div>';
        }
        html += '</div>';
      });

      // Help text
      html += '<div style="margin-top:16px;padding:12px;background:var(--surface);border-radius:8px;font-size:11px;color:var(--text3)">' +
        '💡 A <strong>Beépítem</strong> gomb a kiválasztott MCP szerver URL-jét hozzáadja a lokális config.yaml-hoz. ' +
        'HTTP transportú szerverek közvetlenül használhatóak, STDIO típusúakhoz a parancsot kell telepíteni.</div>';

      return html;
    },

    federation: function(d) {
      if (d.error) return errorBox(d.error);
      var peers = d.peers || [];
      var activeCount = d.active_count || 0;
      var tunnelCount = d.tunnel_count || 0;
      
      var html = '<div style="display:flex;gap:12px;margin-bottom:16px;flex-wrap:wrap;">';
      html += statBox('Peerek', peers.length);
      html += statBox('Aktívak', activeCount, 'var(--success)');
      html += statBox('Tünnelek', tunnelCount, 'var(--primary)');
      html += '</div>';
      
      html += '<div style="display:flex;gap:8px;margin-bottom:16px; justify-content:flex-end;">';
      html += '<button onclick="federationAction(\'discover\')" style="background:var(--primary);color:#fff;border:none;padding:8px 14px;border-radius:6px;cursor:pointer;font-size:12px;font-weight:600">🔍 Felfedezés</button>';
      html += '<button onclick="showFederationAddModal()" style="background:var(--success);color:#fff;border:none;padding:8px 14px;border-radius:6px;cursor:pointer;font-size:12px;font-weight:600">➕ Új Peer</button>';
      html += '</div>';
      
      if (!peers.length) {
        html += empty('Nincs konfigurált federált peer');
      } else {
        html += '<div style="display:grid;grid-template-columns:repeat(auto-fill, minmax(300px, 1fr));gap:12px;">';
        peers.forEach(function(p) {
          var st = p.status || 'unknown';
          var stColor = st === 'online' ? 'var(--success)' : st === 'offline' ? 'var(--danger)' : 'var(--text3)';
          var trustColor = p.trust === 'trusted' ? 'var(--success)' : p.trust === 'untrusted' ? 'var(--danger)' : 'var(--warning)';
          
          var cardInner = '<div style="display:flex;justify-content:space-between;align-items:start;margin-bottom:8px;">' +
            '<div>' +
            '<div style="font-weight:700;font-size:14px;">' + esc(p.name) + ' ' + badge(p.trust, trustColor) + '</div>' +
            '<div style="font-size:12px;color:var(--text3);">' + esc(p.address) + ':' + esc(p.port) + '</div>' +
            '</div>' +
            '<div>' + badge(st, stColor) + '</div>' +
            '</div>' +
            '<div style="display:flex;gap:8px;margin-bottom:12px;font-size:11px;color:var(--text3);">' +
            '<span>Kötés: ' + (p.ssh_tunnel ? 'Tünnel' : 'Közvetlen') + '</span>' +
            '<span>Képességek: ' + (p.capabilities ? p.capabilities.length : 0) + '</span>' +
            '<span>Utolsó: ' + fmtTime(p.last_seen) + '</span>' +
            '</div>' +
            '<div style="display:flex;gap:6px;justify-content:flex-end;">' +
            '<button onclick="federationAction(\'connect\', \'' + esc(p.name) + '\')" style="background:var(--surface2);color:var(--text);border:1px solid var(--border);padding:4px 8px;border-radius:4px;cursor:pointer;font-size:11px;">🔌 Csatlakozás</button>' +
            '<button onclick="federationAction(\'trust\', \'' + esc(p.name) + '\')" style="background:var(--surface2);color:var(--text);border:1px solid var(--border);padding:4px 8px;border-radius:4px;cursor:pointer;font-size:11px;">🛡️ Trust</button>' +
            '<button onclick="federationAction(\'remove\', \'' + esc(p.name) + '\')" style="background:rgba(239,68,68,.2);color:var(--danger);border:1px solid var(--danger);padding:4px 8px;border-radius:4px;cursor:pointer;font-size:11px;">🗑️ Törlés</button>' +
            '</div>' +
            '<div style="text-align:right;margin-top:8px;">' +
            '<a href="#" onclick="showFederationDetail(\'' + esc(p.name) + '\')" style="color:var(--primary);font-size:11px;text-decoration:none;">Részletek ❯</a>' +
            '</div>';
          
          html += card(cardInner);
        });
        html += '</div>';
      }
      return html;
    },

    migrate: function(d) {
      if (d.error) return errorBox(d.error);
      var html = '';
      html += '<div style="display:flex;gap:8px;margin-bottom:12px;"><button onclick="trustAgent()" style="background:var(--primary);color:#fff;border:none;padding:6px 12px;border-radius:6px;cursor:pointer;font-size:11px;">🛡️ Trust Agent</button><button onclick="fleetImport()" style="background:var(--warning);color:#000;border:none;padding:6px 12px;border-radius:6px;cursor:pointer;font-size:11px;">📦 Fleet Import</button></div>';
      var nodes = (d.fleet_status && d.fleet_status.nodes) || d.nodes || [];
      var caps = d.capabilities || [];
      if (caps.length) {
        html += '<div style="display:flex;gap:8px;margin-bottom:16px;">';
        caps.forEach(function(c) { html += badge(c, 'var(--primary)'); });
        html += '</div>';
      }
      if (nodes.length) {
        html += table(['Node', 'Host', 'Port', 'Státusz', 'Heartbeat'], nodes.map(function(n) {
          var st = n.status || 'unknown';
          var sc = st === 'healthy' || st === 'active' ? 'var(--primary)' : 'var(--text3)';
          return [esc(n.node_name || n.name), esc(n.host), esc(n.port || '—'), badge(st, sc), fmtTime(n.last_heartbeat)];
        }));
      } else {
        html += empty('Nincs elérhető node');
      }
      return html;
    },

    docs: function(d) {
      var docs = d.docs || [];
      if (!docs.length) return empty('Nincs dokumentáció');
      var html = '';
      var bySource = {};
      docs.forEach(function(doc) { (bySource[doc.source] = bySource[doc.source] || []).push(doc); });
      Object.keys(bySource).forEach(function(src) {
        html += '<h3 style="margin:0 0 10px;font-size:14px;color:var(--text3);text-transform:uppercase;letter-spacing:.5px;">' + esc(src) + '</h3>';
        bySource[src].forEach(function(doc) {
          html += card('<div onclick="viewDocFile(\'' + esc(doc.name).replace(/'/g, "\\'") + '\',\'' + esc(doc.source).replace(/'/g, "\\'") + '\')" style="cursor:pointer;display:flex;align-items:center;gap:8px;">' +
            '<span style="font-size:16px;">📄</span>' +
            '<strong style="font-size:13px;flex:1;">' + esc(doc.name) + '</strong>' +
            '<span style="font-size:14px;color:var(--text3);">👁️</span></div>' +
            '<div style="font-size:11px;color:var(--text3);margin-top:4px;">' + esc(doc.path) + ' — kattints a megtekintéshez</div>');
        });
        html += '<div style="margin-bottom:16px;"></div>';
      });
      return html;
    },

    overview: function(d) {
      if (d.error) return errorBox(d.error);
      var html = '';
      // Stats row — 4 colorful cards
      var peers = (d.peers || 0) + '/' + (d.peers_total || 0);
      var tasks = d.tasks_total || 0;
      var pending = d.tasks_pending || 0;
      html += '<div style="display:grid;grid-template-columns:repeat(4,1fr);gap:8px;margin-bottom:16px;">';
      html += '<div style="background:var(--surface2);padding:12px;border-radius:10px;text-align:center;border:1px solid var(--border);border-left:3px solid var(--primary);"><div style="font-size:22px;font-weight:700;color:var(--primary);">' + esc(d.node_name || '—') + '</div><div style="font-size:10px;color:var(--text3);margin-top:2px;">Helyi node</div></div>';
      html += '<div style="background:var(--surface2);padding:12px;border-radius:10px;text-align:center;border:1px solid var(--border);border-left:3px solid var(--success);"><div style="font-size:22px;font-weight:700;color:var(--success);">' + esc(String(peers)) + '</div><div style="font-size:10px;color:var(--text3);margin-top:2px;">Peerek</div></div>';
      html += '<div style="background:var(--surface2);padding:12px;border-radius:10px;text-align:center;border:1px solid var(--border);border-left:3px solid var(--warning);"><div style="font-size:22px;font-weight:700;color:var(--warning);">' + esc(String(tasks)) + '</div><div style="font-size:10px;color:var(--text3);margin-top:2px;">Feladatok</div></div>';
      html += '<div style="background:var(--surface2);padding:12px;border-radius:10px;text-align:center;border:1px solid var(--border);border-left:3px solid var(--danger);"><div style="font-size:22px;font-weight:700;color:var(--danger);">' + esc(String(pending)) + '</div><div style="font-size:10px;color:var(--text3);margin-top:2px;">Függőben</div></div>';
      html += '</div>';
      // Node list with icons + version
      var nodes = d.nodes || [];
      if (nodes.length) {
        html += '<h3 style="margin:0 0 10px;font-size:14px;color:var(--text3);text-transform:uppercase;letter-spacing:.5px;">Node-ok (' + nodes.length + ')</h3>';
        html += table(['Node', 'Státusz', 'Verzió', 'Heartbeat'], nodes.map(function(n) {
          var st = n.status || 'unknown';
          var sc = st === 'active' || st === 'healthy' ? 'var(--primary)' : 'var(--text3)';
          var icon = st === 'active' || st === 'healthy' ? '🟢' : '🔴';
          return [icon + ' ' + esc(n.node_name || n.name), badge(st, sc), esc(n.version || '?'), fmtTime(n.last_heartbeat)];
        }));
      }
      // Recent activity with filter
      var act = d.recent_activity || [];
      if (act.length) {
        html += '<h3 style="margin:16px 0 10px;font-size:14px;color:var(--text3);text-transform:uppercase;letter-spacing:.5px;">Legutóbbi aktivitás (' + act.length + ')</h3>';
        html += table(['Feladó', 'Címzett', 'Típus', 'Idő'], act.slice(0, 15).map(function(a) {
          return [esc(a.sender), esc(a.recipient), badge(a.msg_type || 'msg'), fmtTime(a.created_at)];
        }));
        if (act.length > 15) {
          html += '<button onclick="loadMarveenPage(\'activity\')" style="background:var(--surface2);color:var(--text2);border:1px solid var(--border);padding:6px 16px;border-radius:8px;cursor:pointer;font-size:11px;margin-top:8px;">Összes aktivitás →</button>';
        }
      }
      return html || empty('Nincs adat');
    },

    agents: function(d) {
      if (d.error) return errorBox(d.error);
      var agents = d.agents || [];
      if (!agents.length) return empty('Nincs ügynök');
      var html = '';
      agents.forEach(function(a) {
        var st = a.status || 'unknown';
        var sc = st === 'active' || st === 'healthy' ? 'var(--primary)' : 'var(--text3)';
        var caps = a.capabilities || [];
        var skills = a.skills || [];
        if (typeof caps === 'string') { try { caps = JSON.parse(caps); } catch(e) { caps = [caps]; } }
        if (typeof skills === 'string') { try { skills = JSON.parse(skills); } catch(e) { skills = [skills]; } }
        html += card(
          '<div style="display:flex;align-items:center;gap:8px;margin-bottom:8px">' +
          '<span style="font-size:20px;">🤖</span>' +
          '<strong style="font-size:14px;">' + esc(a.node_name || a.name || '—') + '</strong>' +
          badge(st, sc) +
          (a.version ? badge('v' + a.version, 'var(--text3)') : '') +
          '</div>' +
          '<div style="font-size:12px;color:var(--text3);margin-bottom:6px;">' + esc(a.role || '') + ' @ ' + esc(a.host || '—') + ':' + esc(a.p2p_port || '') + '</div>' +
          (caps.length ? '<div style="margin-bottom:6px;"><span style="font-size:11px;color:var(--text3);">Képességek:</span> ' + caps.map(function(c) { return badge(c, 'var(--primary)'); }).join(' ') + '</div>' : '') +
          (skills.length ? '<div><span style="font-size:11px;color:var(--text3);">Skillek:</span> ' + skills.slice(0, 10).map(function(s) { return badge(s, 'var(--surface2)'); }).join(' ') + (skills.length > 10 ? ' +' + (skills.length - 10) : '') + '</div>' : '')
        );
      });
      return html;
    },

    messages: function(d) {
      if (d.error) return errorBox(d.error);
      var msgs = d.messages || [];
      var counts = d.type_counts || [];
      var html = '';
      // Compose button
      html += '<div style="margin-bottom:12px;">';
      html += '<button onclick="showComposeMessage()" style="background:var(--primary);color:#fff;border:none;padding:8px 20px;border-radius:10px;font-size:13px;cursor:pointer;font-weight:600;">✏️ Új üzenet</button>';
      html += '</div>';
      // Type summary
      if (counts.length) {
        html += '<div style="display:flex;gap:6px;margin-bottom:12px;flex-wrap:wrap;">';
        counts.forEach(function(c) {
          html += badge(c.msg_type + ': ' + c.c, 'var(--surface2)');
        });
        html += '</div>';
      }
      if (!msgs.length) return html + empty('Nincs üzenet');
      // Message list — clickable cards
      msgs.forEach(function(m) {
        var pri = m.priority || 'normal';
        var pc = pri === 'high' ? 'var(--danger)' : pri === 'low' ? 'var(--text3)' : 'var(--primary)';
        var mid = esc((m.id || '').toString());
        var typeIcon = {'a2a_message':'💬','delegation':'📋','heartbeat':'💓','skills_announcement':'⭐','agent_reply':'↩️','task_result':'✅','ack':'✓','chat':'💬','directive':'📢','diagnostic_report':'🔧','peer_online':'🟢','peer_offline':'🔴'}[m.msg_type] || '📨';
        html += '<div onclick="showMessageDetail(\'' + mid + '\')" style="background:var(--surface2);border:1px solid var(--border);border-radius:10px;padding:12px;margin-bottom:6px;cursor:pointer;transition:border-color 0.2s;" onmouseover="this.style.borderColor=\'var(--primary)\'" onmouseout="this.style.borderColor=\'var(--border)\'">';
        html += '<div style="display:flex;align-items:center;gap:8px;">';
        html += '<span style="font-size:16px;">' + typeIcon + '</span>';
        html += '<strong style="font-size:12px;color:var(--text2);flex:1;">' + esc(m.sender || '?') + ' → ' + esc(m.recipient || 'broadcast') + '</strong>';
        html += '<span style="font-size:10px;padding:2px 8px;border-radius:4px;background:' + pc + ';color:#fff;font-weight:600;">' + esc(m.msg_type || 'msg') + '</span>';
        html += '<span style="font-size:10px;color:var(--text3);">' + fmtTime(m.created_at) + '</span>';
        html += '</div>';
        html += '<div style="margin-top:4px;font-size:11px;color:var(--text3);">ID: ' + esc((m.id || '').toString().substring(0, 8)) + ' · ' + badge(m.status || '—') + '</div>';
        html += '</div>';
      });
      return html;
    },

    skills: function(d) {
      if (d.error) return errorBox(d.error);
      var skills = d.skills || [];
      if (!skills.length) return empty('Nincs skill');

      // Stats bar
      var totalActive = skills.length;
      var agents = {};
      skills.forEach(function(s) { agents[s.agent || s.node || '—'] = true; });
      var agentCount = Object.keys(agents).length;
      var avgSuccess = 0;
      var avgLatency = 0;
      skills.forEach(function(s) {
        avgSuccess += (s.success_rate || 1);
        avgLatency += (s.avg_latency_ms || 0);
      });
      avgSuccess = totalActive ? (avgSuccess / totalActive * 100).toFixed(0) : 100;
      avgLatency = totalActive ? (avgLatency / totalActive).toFixed(0) : 0;

      var html = '';
      // Toolbar: search + filter + actions
      html += '<div style="display:flex;gap:8px;margin-bottom:12px;flex-wrap:wrap;align-items:center;">' +
        '<input type="text" id="skillSearch" placeholder="🔍 Keresés..." onkeyup="filterSkills()" style="flex:1;min-width:180px;padding:8px 10px;border-radius:6px;background:var(--surface2);border:1px solid var(--border);color:var(--text);font-size:13px;">' +
        '<select id="skillFilterAgent" onchange="filterSkills()" style="padding:8px 10px;border-radius:6px;background:var(--surface2);border:1px solid var(--border);color:var(--text);font-size:13px;">' +
        '<option value="">Összes node</option>' +
        Object.keys(agents).sort().map(function(a) { return '<option value="' + esc(a) + '">' + esc(a) + '</option>'; }).join('') +
        '</select>' +
        '<select id="skillFilterTag" onchange="filterSkills()" style="padding:8px 10px;border-radius:6px;background:var(--surface2);border:1px solid var(--border);color:var(--text);font-size:13px;">' +
        '<option value="">Összes címke</option>' +
        _collectSkillTags(skills).map(function(t) { return '<option value="' + esc(t) + '">' + esc(t) + '</option>'; }).join('') +
        '</select>' +
        '<button onclick="syncSkills()" style="background:var(--primary);color:#fff;border:none;padding:8px 14px;border-radius:6px;cursor:pointer;font-size:12px;white-space:nowrap;">🔄 Szinkron</button>' +
        '</div>';

      // Stats row
      html += '<div style="display:flex;gap:8px;margin-bottom:16px;flex-wrap:wrap;">' +
        '<div style="background:var(--surface2);border-radius:8px;padding:10px 16px;flex:1;min-width:120px;text-align:center;">' +
        '<div style="font-size:24px;font-weight:700;color:var(--primary);">' + totalActive + '</div>' +
        '<div style="font-size:11px;color:var(--text3);">Aktív skill</div></div>' +
        '<div style="background:var(--surface2);border-radius:8px;padding:10px 16px;flex:1;min-width:120px;text-align:center;">' +
        '<div style="font-size:24px;font-weight:700;color:var(--warning);">' + agentCount + '</div>' +
        '<div style="font-size:11px;color:var(--text3);">Node</div></div>' +
        '<div style="background:var(--surface2);border-radius:8px;padding:10px 16px;flex:1;min-width:120px;text-align:center;">' +
        '<div style="font-size:24px;font-weight:700;color:#4ade80;">' + avgSuccess + '%</div>' +
        '<div style="font-size:11px;color:var(--text3);">Átl. siker</div></div>' +
        '<div style="background:var(--surface2);border-radius:8px;padding:10px 16px;flex:1;min-width:120px;text-align:center;">' +
        '<div style="font-size:24px;font-weight:700;color:var(--text2);">' + avgLatency + 'ms</div>' +
        '<div style="font-size:11px;color:var(--text3);">Átl. késleltetés</div></div>' +
        '</div>';

      // Skill cards container
      html += '<div id="skillsGrid" style="display:grid;grid-template-columns:repeat(auto-fill,minmax(300px,1fr));gap:10px;">';

      // Deduplicate by skill_name — show unique skills with agent list
      var byName = {};
      skills.forEach(function(s) {
        var name = s.skill_name || s.skill || s.display_name || '—';
        if (!byName[name]) byName[name] = [];
        byName[name].push(s);
      });

      Object.keys(byName).sort().forEach(function(name) {
        var instances = byName[name];
        var first = instances[0];
        var agentList = instances.map(function(i) { return i.agent || i.node || '—'; });
        var tags = first.tags || [];
        var sr = first.success_rate || 1;
        var srPct = (sr * 100).toFixed(0);
        var srColor = sr >= 0.9 ? '#4ade80' : sr >= 0.7 ? 'var(--warning)' : 'var(--danger)';
        var latency = first.avg_latency_ms || 0;
        var cost = first.cost || 0;
        var desc = first.description || '';
        var skillId = first.skill_id || '';

        html += '<div class="skill-card" data-name="' + esc(name.toLowerCase()) + '" data-agent="' + esc(agentList.join(' ').toLowerCase()) + '" data-tags="' + esc(tags.join(' ').toLowerCase()) + '" style="background:var(--surface2);border-radius:10px;padding:14px;border:1px solid var(--border);cursor:pointer;" onclick="showSkillDetail(\'' + esc(skillId) + '\',\'' + esc(name) + '\')">';
        html += '<div style="display:flex;align-items:flex-start;gap:8px;margin-bottom:6px;">';
        html += '<div style="flex:1;">';
        html += '<strong style="font-size:13px;">' + esc(first.display_name || name) + '</strong>';
        html += '<div style="font-size:11px;color:var(--text3);margin-top:2px;">' + esc(name) + '</div>';
        html += '</div>';
        html += '<div style="background:' + srColor + ';color:#000;border-radius:4px;padding:2px 6px;font-size:10px;font-weight:600;">' + srPct + '%</div>';
        html += '</div>';

        if (desc) {
          html += '<div style="font-size:11px;color:var(--text2);line-height:1.4;margin-bottom:8px;">' + esc(desc.substring(0, 100)) + (desc.length > 100 ? '...' : '') + '</div>';
        }

        // Agent badges
        html += '<div style="display:flex;flex-wrap:wrap;gap:4px;margin-bottom:6px;">';
        agentList.slice(0, 4).forEach(function(a) {
          html += '<span style="background:rgba(79,140,255,.15);color:var(--primary);padding:2px 7px;border-radius:4px;font-size:10px;">🤖 ' + esc(a) + '</span>';
        });
        if (agentList.length > 4) html += '<span style="color:var(--text3);font-size:10px;">+' + (agentList.length - 4) + '</span>';
        html += '</div>';

        // Tags
        if (tags.length) {
          html += '<div style="display:flex;flex-wrap:wrap;gap:3px;margin-bottom:6px;">';
          tags.slice(0, 5).forEach(function(t) {
            html += '<span style="background:var(--surface);color:var(--text3);padding:1px 6px;border-radius:3px;font-size:9px;">#' + esc(t) + '</span>';
          });
          html += '</div>';
        }

        // Metrics row
        html += '<div style="display:flex;gap:12px;font-size:10px;color:var(--text3);border-top:1px solid var(--border);padding-top:6px;">';
        html += '<span>⚡ ' + esc(latency) + 'ms</span>';
        html += '<span>💰 $' + esc(cost.toFixed(4)) + '</span>';
        html += '<span>📦 ' + esc(first.max_concurrent || 1) + ' concurrent</span>';
        html += '</div>';

        html += '</div>';
      });

      html += '</div>';
      html += '<div id="skillsEmpty" style="display:none;text-align:center;padding:40px;color:var(--text3);">Nincs találat</div>';

      return html;
    },

    tasks: function(d) {
      if (d.error) return errorBox(d.error);
      var tasks = d.tasks || [];
      var html = '';
      html += '<div style="display:flex;gap:8px;margin-bottom:12px;">';
      html += '<button onclick="showLlmBreakdownModal()" style="background:var(--primary);color:#fff;border:none;padding:6px 12px;border-radius:6px;cursor:pointer;font-size:11px;">🧠 LLM Breakdown</button>';
      html += '<button onclick="taskCleanup()" style="background:var(--warning);color:#000;border:none;padding:6px 12px;border-radius:6px;cursor:pointer;font-size:11px;">🧹 Cleanup</button>';
      html += '</div>';
      if (!tasks.length) return html + empty('Nincs ütemezett feladat');
      html += table(['ID', 'Feladó', 'Címzett', 'Tárgy', 'Státusz', 'Prioritás', 'Típus', 'Létrehozva'], tasks.map(function(t) {
        var st = t.status || 'unknown';
        var sc = st === 'completed' ? 'var(--primary)' : st === 'running' || st === 'accepted' ? 'var(--warning)' : st === 'failed' || st === 'expired' ? 'var(--danger)' : 'var(--text3)';
        var pri = t.priority || 'normal';
        var pc = pri === 'high' ? 'var(--danger)' : pri === 'low' ? 'var(--text3)' : 'var(--primary)';
        return [esc((t.id || '').toString().substring(0, 8)), esc(t.from_agent || '—'), esc(t.to_agent || '—'), esc(t.subject || '—'), badge(st, sc), badge(pri, pc), badge(t.task_type || '—'), fmtTime(t.created_at)];
      }));
      return html;
    },
    'insights-cost': function(d) {
      if (d.error) return errorBox(d.error);
      var html = '';
      var s = d.summary || {};
      // Summary cards
      html += '<div style="display:grid;grid-template-columns:repeat(4,1fr);gap:8px;margin-bottom:16px;">';
      html += '<div style="background:var(--surface2);padding:12px;border-radius:8px;text-align:center;border:1px solid var(--border);"><div style="font-size:20px;font-weight:700;color:var(--primary);">$' + esc(String((s.total_cost_usd || 0).toFixed(4))) + '</div><div style="font-size:10px;color:var(--text3);">Havi költség</div></div>';
      html += '<div style="background:var(--surface2);padding:12px;border-radius:8px;text-align:center;border:1px solid var(--border);"><div style="font-size:20px;font-weight:700;color:var(--text);">' + esc(String(s.total_requests || 0)) + '</div><div style="font-size:10px;color:var(--text3);">Kérések</div></div>';
      html += '<div style="background:var(--surface2);padding:12px;border-radius:8px;text-align:center;border:1px solid var(--border);"><div style="font-size:20px;font-weight:700;color:var(--text);">' + esc(String((s.total_input_tokens || 0) + (s.total_output_tokens || 0))) + '</div><div style="font-size:10px;color:var(--text3);">Tokenek</div></div>';
      html += '<div style="background:var(--surface2);padding:12px;border-radius:8px;text-align:center;border:1px solid var(--border);"><div style="font-size:20px;font-weight:700;color:var(--text2);">' + esc(s.month || '—') + '</div><div style="font-size:10px;color:var(--text3);">Hónap</div></div>';
      html += '</div>';
      // By agent
      var byAgent = s.by_agent || {};
      var agentKeys = Object.keys(byAgent);
      if (agentKeys.length) {
        html += '<h3 style="margin:0 0 8px;font-size:13px;">💰 Költség agentenként</h3>';
        html += table(['Agent', 'Költség'], agentKeys.map(function(name) {
          return [esc(name), '$' + esc(String((byAgent[name] || 0).toFixed(4)))];
        }));
      }
      // By model
      var byModel = s.by_model || {};
      var modelKeys = Object.keys(byModel);
      if (modelKeys.length) {
        html += '<h3 style="margin:16px 0 8px;font-size:13px;">🤖 Költség modellenként</h3>';
        html += table(['Modell', 'Költség'], modelKeys.map(function(name) {
          return [esc(name), '$' + esc(String((byModel[name] || 0).toFixed(4)))];
        }));
      }
      // Alerts
      var alerts = d.alerts || [];
      if (alerts.length) {
        html += '<h3 style="margin:16px 0 8px;font-size:13px;">⚠️ Költségfigyelő</h3>';
        alerts.forEach(function(a) {
          var c = a.level === 'red' ? 'var(--danger)' : a.level === 'yellow' ? 'var(--warning)' : 'var(--success)';
          html += card('<div style="display:flex;align-items:center;gap:8px">' + badge(a.level.toUpperCase(), c) + '<span>' + esc(a.message) + '</span></div>');
        });
      }
      html += '<canvas id="costChartCanvas" style="width:100%;height:200px;margin-top:12px;border-radius:8px;"></canvas>';
      setTimeout(function() { window.loadCostChart && window.loadCostChart(); }, 50);
      return html;
    },
    'insights-inbox': function(d) {
      if (d.error) return errorBox(d.error);
      var html = '<div style=\"display:flex;gap:12px;margin-bottom:16px;flex-wrap:wrap;\">';
      html += '<div style=\"background:var(--surface2);border:1px solid var(--border);padding:8px 16px;border-radius:8px;text-align:center;flex:1;min-width:100px\"><div style=\"font-size:20px;font-weight:700;color:var(--text)\">' + (d.total_unread || 0) + '</div><div style=\"font-size:11px;color:var(--text3)\">Olvasatlan</div></div>';
      html += '<div style=\"background:var(--surface2);border:1px solid var(--border);padding:8px 16px;border-radius:8px;text-align:center;flex:1;min-width:100px\"><div style=\"font-size:20px;font-weight:700;color:var(--warning)\">' + (d.nudged || 0) + '</div><div style=\"font-size:11px;color:var(--text3)\">Emlékeztető</div></div>';
      html += '<div style=\"background:var(--surface2);border:1px solid var(--border);padding:8px 16px;border-radius:8px;text-align:center;flex:1;min-width:100px\"><div style=\"font-size:20px;font-weight:700;color:var(--danger)\">' + (d.alerted || 0) + '</div><div style=\"font-size:11px;color:var(--text3)\">Sürgős</div></div>';
      html += '</div>';
      var msgs = d.messages || [];
      if (msgs.length) {
        html += '<h3 style=\"margin-bottom:12px\">📥 Üzenetek</h3>';
        html += table(['Címzett', 'Feladó', 'Idő', 'Képek', 'Státusz'], msgs.map(function(m) {
          var status = '';
          if (m.alerted) status += badge('SÜRGŐS', 'var(--danger)') + ' ';
          if (m.nudged) status += badge('EMLÉKEZTETŐ', 'var(--warning)');
          return [esc(m.to_node), esc(m.from_node), esc(m.age_min + ' perc'), esc(m.preview), status];
        }));
      }
      return html || empty('Nincs beérkező üzenet');
    },
    'insights-context-gate': function(d) {
      if (d.error) return errorBox(d.error);
      var agents = d.agents || [];
      if (!agents.length) return empty('Nincs aktív delegation — minden agent tétlen (healthy)');

      // Summary bar
      var s = d.summary || {};
      var th = d.thresholds || {};
      var html = '<div style="padding:16px;">';

      // Header
      html += '<h2 style="margin:0 0 16px;font-size:18px;">🧠 Kontextus Kapu</h2>';

      // Summary cards
      html += '<div style="display:grid;grid-template-columns:repeat(4,1fr);gap:8px;margin-bottom:16px;">';
      html += '<div style="background:var(--surface2);padding:12px;border-radius:8px;text-align:center;border:1px solid var(--border);">';
      html += '<div style="font-size:20px;font-weight:700;color:var(--primary);">' + (s.total || 0) + '</div>';
      html += '<div style="font-size:10px;color:var(--text3);">Összes agent</div></div>';
      html += '<div style="background:var(--surface2);padding:12px;border-radius:8px;text-align:center;border:1px solid var(--success);">';
      html += '<div style="font-size:20px;font-weight:700;color:var(--success);">' + (s.healthy || 0) + '</div>';
      html += '<div style="font-size:10px;color:var(--text3);">Healthy</div></div>';
      html += '<div style="background:var(--surface2);padding:12px;border-radius:8px;text-align:center;border:1px solid var(--warning);">';
      html += '<div style="font-size:20px;font-weight:700;color:var(--warning);">' + (s.warning || 0) + '</div>';
      html += '<div style="font-size:10px;color:var(--text3);">Warning</div></div>';
      html += '<div style="background:var(--surface2);padding:12px;border-radius:8px;text-align:center;border:1px solid var(--danger);">';
      html += '<div style="font-size:20px;font-weight:700;color:var(--danger);">' + (s.critical || 0) + '</div>';
      html += '<div style="font-size:10px;color:var(--text3);">Critical</div></div>';
      html += '</div>';

      // Threshold info
      html += '<div style="background:var(--surface2);border-radius:8px;padding:10px;margin-bottom:16px;font-size:11px;color:var(--text3);">';
      html += 'Thresholds: Soft ' + (th.soft || 70) + '% | Hard ' + (th.hard || 85) + '% | Critical ' + (th.critical || 95) + '%';
      html += '</div>';

      // Agent cards
      html += '<h3 style="margin:0 0 10px;font-size:14px;color:var(--text3);text-transform:uppercase;letter-spacing:.5px;">Agent státuszok</h3>';
      agents.forEach(function(a) {
        var pct = a.pct || 0;
        var sev = a.severity || 'ok';
        var color = sev === 'critical' ? 'var(--danger)' : sev === 'warning' ? 'var(--warning)' : 'var(--success)';
        var bgColor = sev === 'critical' ? 'rgba(220,53,69,0.1)' : sev === 'warning' ? 'rgba(255,193,7,0.1)' : 'var(--surface2)';
        var icon = sev === 'critical' ? '🔴' : sev === 'warning' ? '🟡' : '🟢';

        html += '<div style="background:' + bgColor + ';border-radius:10px;padding:14px;margin-bottom:10px;border:1px solid ' + color + ';">';

        // Agent header
        html += '<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:10px;">';
        html += '<div style="display:flex;align-items:center;gap:8px;">';
        html += '<span style="font-size:14px;">' + icon + '</span>';
        html += '<strong style="font-size:14px;">' + esc(a.agent) + '</strong>';
        html += '</div>';
        html += '<div style="text-align:right;"><div style="font-size:12px;color:var(--text2);">' + (a.est_tokens || 0) + ' tokens</div>';
        if (a.model && a.model !== 'unknown') {
          html += '<div style="font-size:10px;color:var(--text3);word-break:break-word;max-width:220px;margin-top:2px;">🛠️ ' + esc(a.model) + '</div>';
        }
        html += '</div>';
        html += '</div>';

        // Progress bar
        html += '<div style="background:var(--border);height:8px;border-radius:4px;overflow:hidden;margin-bottom:8px;">';
        html += '<div style="background:' + color + ';height:100%;width:' + Math.min(pct, 100) + '%;transition:width .3s;border-radius:4px;"></div>';
        html += '</div>';

        // Stats grid
        html += '<div style="display:grid;grid-template-columns:repeat(4,1fr);gap:6px;font-size:11px;">';
        html += '<div style="text-align:center;"><div style="color:var(--text2);font-weight:600;">' + (a.turns || 0) + '/' + (a.max_turns || 90) + '</div><div style="color:var(--text3);font-size:9px;">turns</div></div>';
        html += '<div style="text-align:center;"><div style="color:var(--text2);font-weight:600;">' + pct + '%</div><div style="color:var(--text3);font-size:9px;">saturation</div></div>';
        html += '<div style="text-align:center;"><div style="color:var(--text2);font-weight:600;">' + (a.running || 0) + '/' + (a.pending || 0) + '</div><div style="color:var(--text3);font-size:9px;">run/pending</div></div>';
        html += '<div style="text-align:center;"><div style="color:var(--text2);font-weight:600;">' + (a.active_delegations || 0) + '</div><div style="color:var(--text3);font-size:9px;">delegations</div></div>';
        html += '</div>';

        // Action message
        if (a.message) {
          html += '<div style="margin-top:8px;padding:6px 10px;background:rgba(0,0,0,0.15);border-radius:6px;font-size:11px;color:' + color + ';font-weight:500;">' + esc(a.message) + '</div>';
        }

        html += '</div>';
      });

      html += '</div>';      // Context CRUD
      html += '<div style="margin-top:16px;display:flex;gap:8px;"><button onclick="showContextAddModal()" style="background:var(--primary);color:#fff;border:none;padding:6px 12px;border-radius:6px;cursor:pointer;font-size:11px;">➕ Context set</button></div>';

      return html;
    },
    'shared-context': function(d) {
      if (d.error) return errorBox(d.error);
      var entries = d.entries || (Array.isArray(d) ? d : []);
      var html = '<div style="padding:16px;">';
      html += '<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:16px;gap:12px;">';
      html += '  <div style="display:flex;gap:8px;flex:1;max-width:400px;">';
      html += '    <input id="ctxPrefix" type="text" placeholder="Szűrés prefix alapján..." style="flex:1;padding:8px 12px;border-radius:8px;border:1px solid var(--border);background:var(--surface2);color:var(--text);font-size:13px;">';
      html += '    <button onclick="refreshSharedContext()" style="background:var(--surface2);color:var(--text2);border:1px solid var(--border);padding:8px 12px;border-radius:8px;cursor:pointer;font-size:13px;">🔄 Frissítés</button>';
      html += '  </div>';
      html += '  <button onclick="showSharedContextModal()" style="background:var(--primary);color:#fff;border:none;padding:8px 16px;border-radius:8px;cursor:pointer;font-size:13px;font-weight:600;">➕ Új bejegyzés</button>';
      html += '</div>';
      if (!entries.length) {
        html += empty('Nincs található Shared Context bejegyzés');
      } else {
        var rows = entries.map(function(e) {
          var val = String(e.value || '');
          var truncatedVal = val.length > 50 ? val.substring(0, 47) + '...' : val;
          var keyEsc = esc(e.key);
          return [
            '<strong>' + keyEsc + '</strong>',
            '<div title="' + esc(val) + '" style="word-break:break-all;max-width:300px;">' + esc(truncatedVal) + '</div>',
            badge(e.value_type || 'text', e.value_type === 'json' ? 'var(--warning)' : 'var(--primary)'),
            esc(e.agent || '—'),
            fmtTime(e.updated_at),
            '<div style="display:flex;gap:6px;">' +
            '<button onclick="showSharedContextModal(' + JSON.stringify(e).replace(/'/g, "&apos;") + ')" style="background:none;border:1px solid var(--border);color:var(--text2);padding:4px 8px;border-radius:4px;cursor:pointer;font-size:11px;">Kezelés</button>' +
            '<button onclick="deleteSharedContext(\'' + keyEsc + '\')" style="background:none;border:1px solid var(--danger);color:var(--danger);padding:4px 8px;border-radius:4px;cursor:pointer;font-size:11px;">Törlés</button>' +
            '</div>'
          ];
        });
        html += table(['Kulcs', 'Érték', 'Típus', 'Agent', 'Frissítve', 'Műveletek'], rows);
      }
      html += '</div>';
      return html;
    },
    'insights-conversations-log': function(d) {
      if (d.error) return errorBox(d.error);
      var msgs = d.messages || [];
      if (!msgs.length) return empty('Nincs üzenet');
      var html = '<div style=\"display:flex;flex-direction:column;gap:8px;padding:10px;max-height:600px;overflow-y:auto;\">';
      msgs.slice(-50).forEach(function(m) {
        var isUser = m.role === 'user';
        var align = isUser ? 'flex-start' : 'flex-end';
        var bg = isUser ? 'var(--primary-dim)' : 'var(--success)';
        var color = '#fff';
        html += '<div style=\"display:flex;justify-content:' + align + ';\">' + 
          '<div style=\"max-width:80%;padding:8px 12px;border-radius:12px;background:' + bg + ';color:' + color + ';font-size:13px;line-height:1.4;word-wrap:break-word;border-bottom-right-radius:' + (isUser ? '12px' : '4px') + ';border-bottom-left-radius:' + (isUser ? '4px' : '12px') + '\">' + 
          '<div style=\"font-size:10px;opacity:.8;margin-bottom:4px;font-weight:600;\">' + esc(m.role === 'user' ? 'Felhasználó' : 'Asszisztens') + '</div>' + 
          '<div>' + esc(m.content) + '</div>' + 
          '<div style=\"font-size:10px;opacity:.6;margin-top:4px;text-align:right;\">' + fmtTime(m.timestamp) + '</div>' + 
          '</div></div>';
      });
      html += '</div>';
      return html;
    },
    'projects': function(d) {
      if (d.error) return errorBox(d.error);
      var projects = d.projects || [];
      var count = d.count || projects.length;
      var html = '';
      html += '<div style="display:flex;gap:8px;margin-bottom:16px;flex-wrap:wrap;">';
      html += '<div style="background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:8px 14px;text-align:center;min-width:80px;"><div style="font-size:18px;font-weight:700;color:var(--primary);">' + count + '</div><div style="font-size:10px;color:var(--text3);">Projektek</div></div>';
      html += '</div>';
      if (!projects.length) return html + empty('Nincs projekt');
      html += '<div style="display:flex;flex-direction:column;gap:8px;">';
      projects.forEach(function(p) {
        var statusColor = p.status === 'up' ? 'var(--success)' : p.status === 'down' ? 'var(--danger)' : 'var(--text3)';
        html += card('<div style="display:flex;align-items:flex-start;gap:8px;">' +
          '<span style="font-size:20px;flex-shrink:0;">' + esc(p.icon || '📦') + '</span>' +
          '<div style="flex:1;"><strong style="font-size:13px;">' + esc(p.title || p.id || '—') + '</strong>' +
          '<div style="font-size:10px;color:var(--text3);margin-top:2px;">' + esc(p.description || '') + '</div>' +
          '<div style="display:flex;gap:6px;margin-top:4px;">' +
          '<span style="font-size:9px;padding:1px 6px;border-radius:4px;background:' + statusColor + '22;color:' + statusColor + ';">' + esc(p.status || '?') + '</span>' +
          '<span style="font-size:9px;color:var(--text3);">URL: ' + esc(p.url || '—') + '</span>' +
          '<button onclick="projectDelete(\'' + esc(p.id || p._id || '') + '\')" style="font-size:9px;background:rgba(239,68,68,.2);color:var(--danger);border:1px solid var(--danger);padding:2px 6px;border-radius:4px;cursor:pointer;margin-left:4px;">🗑️</button>' +
          '</div></div></div>');
      });
      html += '</div>';
      // Action buttons
      html += '<div style="display:flex;gap:8px;margin-bottom:8px;">';
      html += '<button onclick="showProjectAddModal()" style="background:var(--primary);color:#fff;border:none;padding:6px 12px;border-radius:6px;cursor:pointer;font-size:11px;">➕ Új projekt</button>';
      html += '<button onclick="projectSync()" style="background:var(--surface2);color:var(--text);border:1px solid var(--border);padding:6px 12px;border-radius:6px;cursor:pointer;font-size:11px;">🔄 Sync</button>';
      html += '</div>';
      // Team section (loaded via JS)
      html += '<div id="projects-team-section" style="margin-top:16px;"></div>';
      // Workflows section
      html += '<div id="projects-workflows-section" style="margin-top:16px;"></div>';
      return html;
    },
    'network': function(d) {
      if (d.error) return errorBox(d.error);
      var running = d.running;
      var listenPort = d.listen_port || '?';
      var tlsEnabled = d.tls_enabled;
      var peers = d.peers || [];
      var peerCount = d.peer_count || 0;
      var backoffPeers = d.backoff_peers || [];
      var html = '';
      // P2P status
      var statusColor = running ? 'var(--success)' : 'var(--danger)';
      html += '<div style="background:var(--surface2);border-radius:8px;padding:14px;margin-bottom:16px;border-left:4px solid ' + statusColor + ';">';
      html += '<div style="display:flex;align-items:center;gap:8px;margin-bottom:8px;">';
      html += '<div style="width:10px;height:10px;border-radius:50%;background:' + statusColor + ';"></div>';
      html += '<strong style="font-size:14px;color:' + statusColor + ';">' + (running ? 'P2P Aktív' : 'P2P Leállt') + '</strong>';
      html += '</div>';
      html += '<div style="display:flex;gap:16px;font-size:11px;color:var(--text3);">';
      html += '<span>Port: <strong>' + esc(listenPort) + '</strong></span>';
      html += '<span>TLS: <strong>' + (tlsEnabled ? 'Igen' : 'Nem') + '</strong></span>';
      html += '<span>Peers: <strong>' + peerCount + '</strong></span>';
      html += '</div></div>';
      // Peers
      if (peers.length) {
        html += '<h3 style="margin:0 0 8px;font-size:13px;">🔗 Peer-ek (' + peers.length + ')</h3>';
        peers.forEach(function(p) {
          html += card('<div style="display:flex;align-items:center;gap:8px;"><div style="width:8px;height:8px;border-radius:50%;background:var(--success);flex-shrink:0;"></div><strong style="font-size:12px;flex:1;">' + esc(p) + '</strong></div>');
        });
      }
      // Backoff peers
      if (backoffPeers && backoffPeers.length) {
        html += '<h3 style="margin:16px 0 8px;font-size:13px;color:var(--warning);">⚠️ Backoff peer-ek (' + backoffPeers.length + ')</h3>';
        backoffPeers.forEach(function(p) {
          html += card('<div style="display:flex;align-items:center;gap:8px;"><div style="width:8px;height:8px;border-radius:50%;background:var(--warning);flex-shrink:0;"></div><strong style="font-size:12px;flex:1;">' + esc(typeof p === 'string' ? p : (p.name || p.peer || '?')) + '</strong></div>');
        });
      }
      // Extra sections loaded via JS
      html += '<div id="network-mcp-section" style="margin-top:16px;"></div>';
      html += '<div id="network-watchdog-section" style="margin-top:16px;"></div>';
      html += '<div id="network-desired-section" style="margin-top:16px;"></div>';
      html += '<div id="network-channel-section" style="margin-top:16px;"></div>';
      return html;
    },
    'security': function(d) {
      if (d.error) return errorBox(d.error);
      var checks = d.checks || [];
      var html = '';
      html += '<h3 style="margin:0 0 8px;font-size:13px;">🛡️ Context Guard ellenőrzések</h3>';
      if (!checks.length) {
        html += card('<div style="text-align:center;color:var(--text3);font-size:12px;padding:12px;">✅ Nincs aktív kontextus probléma</div>');
      } else {
        checks.forEach(function(c) {
          var sevColor = c.severity === 'critical' ? 'var(--danger)' : c.severity === 'warning' ? 'var(--warning)' : 'var(--text3)';
          html += card('<div style="display:flex;align-items:flex-start;gap:8px;"><span style="font-size:9px;padding:2px 6px;border-radius:4px;background:' + sevColor + ';color:#fff;font-weight:600;flex-shrink:0;">' + esc(c.severity || 'info') + '</span><div style="flex:1;"><strong style="font-size:12px;">' + esc(c.name || c.check || '—') + '</strong><div style="font-size:10px;color:var(--text3);margin-top:2px;">' + esc(c.message || c.detail || '') + '</div></div></div>');
        });
      }
      // Extra sections
      html += '<div id="security-memory-section" style="margin-top:16px;"></div>';
      html += '<div id="security-throttle-section" style="margin-top:16px;"></div>';
      html += '<div id="security-lock-section" style="margin-top:16px;"></div>';
      html += '<div id="security-recovery-section" style="margin-top:16px;"></div>';
      // Session management section
      html += '<div style="margin-top:16px;">';
      html += '<h3 style="margin:0 0 8px;font-size:13px;">🔑 Munkamenetek (Sessions)</h3>';
      html += '<div style="background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:14px;">';
      html += '<div style="font-size:12px;color:var(--text3);margin-bottom:8px;">A token lejárati ideje órában. 0 = soha nem jár le (végtelen).</div>';
      html += '<div style="display:flex;gap:8px;align-items:center;margin-bottom:12px;">';
      html += '<label style="font-size:12px;color:var(--text2);">Saját session timeout:</label>';
      html += '<input id="sessionTimeoutInput" type="number" min="0" max="87600" step="1" value="24" style="width:80px;background:var(--bg);border:1px solid var(--border);color:var(--text);padding:6px 8px;border-radius:6px;font-size:12px;" />';
      html += '<span style="font-size:11px;color:var(--text3);">óra (0=végtelen)</span>';
      html += '<button onclick="saveSessionTimeout()" style="background:var(--primary);color:#fff;border:none;padding:6px 14px;border-radius:6px;cursor:pointer;font-size:12px;">Mentés</button>';
      html += '</div>';
      html += '<div id="sessionListContainer" style="margin-top:12px;"></div>';
      html += '</div></div>';
      return html;
    },
    'sysinfo': function(d) {
      if (d.error) return errorBox(d.error);
      var timeouts = d.timeouts || {};
      var categories = d.categories || [];
      var html = '';
      // Tool timeouts
      html += '<h3 style="margin:0 0 8px;font-size:13px;">⏱️ Tool Timeout-ok</h3>';
      var timeoutItems = Object.keys(timeouts);
      if (timeoutItems.length) {
        html += '<div style="display:flex;flex-direction:column;gap:4px;">';
        timeoutItems.forEach(function(key) {
          html += card('<div style="display:flex;align-items:center;gap:8px;"><strong style="font-size:11px;flex:1;">' + esc(key) + '</strong><span style="font-size:12px;font-weight:700;color:var(--primary);">' + esc(timeouts[key]) + 's</span></div>');
        });
        html += '</div>';
      } else {
        html += empty('Nincs timeout beállítás');
      }
      // Extra sections
      html += '<div id="sysinfo-queue-section" style="margin-top:16px;"></div>';
      html += '<div id="sysinfo-retries-section" style="margin-top:16px;"></div>';
      html += '<div id="sysinfo-store-section" style="margin-top:16px;"></div>';
      html += '<div id="sysinfo-workers-section" style="margin-top:16px;"></div>';
      html += '<div id="sysinfo-model-section" style="margin-top:16px;"></div>';
      html += '<div id="sysinfo-db-section" style="margin-top:16px;"></div>';
      html += '<div id="sysinfo-labels-section" style="margin-top:16px;"></div>';
      html += '<div id="sysinfo-voice-section" style="margin-top:16px;"></div>';
      html += '<div style="margin-top:16px;display:flex;gap:8px;"><button onclick="authSync()" style="background:var(--surface2);color:var(--text);border:1px solid var(--border);padding:6px 12px;border-radius:6px;cursor:pointer;font-size:11px;">🔐 Auth Sync</button><button onclick="webhookDeploy()" style="background:var(--warning);color:#000;border:none;padding:6px 12px;border-radius:6px;cursor:pointer;font-size:11px;">🪝 Webhook Deploy</button></div>';
      return html;
    },
    'nodes': function(d) {
      if (d.error) return errorBox(d.error);
      var nodes = d.nodes || [];
      if (!nodes.length) return empty('Nincs node adat');
      var html = '<div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(200px,1fr));gap:8px;">';
      nodes.forEach(function(n) {
        var online = n.status === 'active' || n.status === 'online';
        var dotColor = online ? 'var(--success)' : 'var(--danger)';
        html += '<div style="background:var(--surface2);border-radius:10px;padding:12px;border:1px solid var(--border);">';
        html += '<div style="display:flex;align-items:center;gap:6px;margin-bottom:8px;">';
        html += '<div style="width:8px;height:8px;border-radius:50%;background:' + dotColor + ';"></div>';
        html += '<strong style="font-size:13px;">' + esc(n.node_name || n.name || '?') + '</strong>';
        html += '</div>';
        html += '<div style="font-size:11px;color:var(--text3);">v' + esc(n.version || '?') + ' • ' + esc(n.host || '') + '</div>';
        if (n.capabilities) html += '<div style="font-size:10px;color:var(--text2);margin-top:4px;">' + esc(n.capabilities.length) + ' képesség</div>';
        html += '</div>';
      });
      html += '</div>';
      return html;
    },
    'ideas': function(d) {
      if (d.error) return errorBox(d.error);
      var ideas = d.ideas || d.items || [];
      var html = '<button onclick="showIdeaForm()" style="background:var(--primary);color:#fff;border:none;padding:8px 16px;border-radius:8px;cursor:pointer;font-size:12px;margin-bottom:12px;">+ Új ötlet</button>';
      if (!ideas.length) return html + empty('Még nincs ötlet');
      ideas.forEach(function(i) {
        var status = i.status || 'open';
        var sColor = status === 'approved' ? 'var(--success)' : status === 'rejected' ? 'var(--danger)' : 'var(--warning)';
        html += '<div style="background:var(--surface2);border-radius:10px;padding:12px;margin-bottom:8px;border:1px solid var(--border);">';
        html += '<div style="display:flex;justify-content:space-between;align-items:start;">';
        html += '<strong style="font-size:13px;">' + esc(i.title || '—') + '</strong>';
        html += '<span style="font-size:10px;padding:2px 8px;border-radius:4px;background:' + sColor + ';color:#fff;">' + esc(status) + '</span>';
        html += '</div>';
        if (i.description) html += '<div style="font-size:11px;color:var(--text3);margin-top:4px;">' + esc(i.description.substring(0, 120)) + '</div>';
        if (i.tags) html += '<div style="font-size:10px;color:var(--text2);margin-top:4px;">' + esc(i.tags) + '</div>';
        html += '</div>';
      });
      return html;
    },
    'labels': function(d) {
      if (d.error) return errorBox(d.error);
      var labels = d.labels || d.items || [];
      if (!labels.length) return empty('Nincs címke');
      var html = '<div style="display:flex;flex-wrap:wrap;gap:8px;">';
      labels.forEach(function(l) {
        var color = l.color || 'var(--primary)';
        html += '<div style="background:' + color + ';color:#fff;padding:4px 12px;border-radius:12px;font-size:11px;font-weight:600;">' + esc(l.name || l.label || '?') + '</div>';
      });
      html += '</div>';
      return html;
    },
    'files': function(d) {
      if (d.error) return errorBox(d.error);
      var files = d.files || [];
      if (!files.length) return empty('Nincs fájl');
      var html = '<div style="display:flex;flex-direction:column;gap:6px;">';
      files.forEach(function(f) {
        html += '<div style="background:var(--surface2);border-radius:8px;padding:10px;display:flex;align-items:center;gap:10px;border:1px solid var(--border);">';
        html += '<span style="font-size:20px;">📄</span>';
        html += '<div style="flex:1;"><div style="font-size:12px;font-weight:600;">' + esc(f.name || '?') + '</div>';
        html += '<div style="font-size:10px;color:var(--text3);">' + esc(f.size_human || f.size || '?') + ' • ' + esc(f.type || '') + '</div></div>';
        if (f.url) html += '<a href="' + esc(f.url) + '" target="_blank" style="font-size:11px;color:var(--primary);text-decoration:none;">⬇️</a>';
        html += '</div>';
      });
      html += '</div>';
      return html;
    },
    'workflow': function(d) {
      if (d.error) return errorBox(d.error);
      var workflows = d.workflows || d.items || [];
      var html = '<button onclick="showWorkflowForm()" style="background:var(--primary);color:#fff;border:none;padding:8px 16px;border-radius:8px;cursor:pointer;font-size:12px;margin-bottom:12px;">+ Új workflow</button>';
      if (!workflows.length) return html + empty('Nincs workflow');
      workflows.forEach(function(w) {
        var steps = w.steps || [];
        html += '<div style="background:var(--surface2);border-radius:10px;padding:12px;margin-bottom:8px;border:1px solid var(--border);">';
        html += '<strong style="font-size:13px;">' + esc(w.name || w.id || '?') + '</strong>';
        if (w.description) html += '<div style="font-size:11px;color:var(--text3);margin-top:4px;">' + esc(w.description) + '</div>';
        html += '<div style="font-size:10px;color:var(--text2);margin-top:4px;">' + steps.length + ' lépés</div>';
        html += '</div>';
      });
      return html;
    },

    'chat': function(d) {
      if (d.error) return errorBox(d.error);
      var contacts = d.contacts || [];
      var html = '<div style="display:flex;gap:12px;height:calc(100vh - 200px);min-height:400px;">';
      html += '<div id="chatContactList" style="width:240px;min-width:240px;overflow-y:auto;background:var(--surface2);border-radius:10px;padding:8px;">';
      html += '<div style="font-size:12px;font-weight:600;color:var(--text3);padding:8px 4px 12px;">Beszélgetések</div>';
      contacts.forEach(function(c) {
        var name = esc(c.agent || '?');
        var rawName = String(c.agent || '?').replace(/'/g, '');
        var unread = c.unread || 0;
        var lastMsg = c.last_msg ? fmtTime(c.last_msg) : '';
        var icon = c.is_user ? '👤' : '🤖';
        var badgeHtml = unread > 0 ? '<span style="background:var(--danger);color:#fff;font-size:10px;padding:1px 6px;border-radius:10px;margin-left:4px;">' + unread + '</span>' : '';
        var activeCls = (window._chatActiveContact === c.agent) ? 'border:2px solid var(--primary);' : 'border:1px solid var(--border);';
        html += '<div data-chat-agent="' + rawName + '" class="chat-contact-item" style="cursor:pointer;padding:10px;border-radius:8px;margin-bottom:4px;' + activeCls + 'background:var(--surface);transition:border-color 0.2s;">';
        html += '<div style="font-size:13px;font-weight:600;color:var(--text);">' + icon + ' ' + name + badgeHtml + '</div>';
        if (lastMsg) html += '<div style="font-size:10px;color:var(--text3);">' + lastMsg + '</div>';
        html += '</div>';
      });
      if (!contacts.length) html += '<div style="color:var(--text3);padding:12px;font-size:12px;">Nincs kontakt</div>';
      html += '</div>';
      html += '<div id="chatArea" style="flex:1;display:flex;flex-direction:column;background:var(--surface2);border-radius:10px;overflow:hidden;">';
      html += '<div id="chatHeader" style="padding:12px 16px;border-bottom:1px solid var(--border);font-size:14px;font-weight:600;color:var(--text);">💬 Válassz egy kontaktot a bal oldalon</div>';
      html += '<div id="chatMessages" style="flex:1;overflow-y:auto;padding:12px;display:flex;flex-direction:column;justify-content:center;align-items:center;color:var(--text3);font-size:13px;">← Kattints egy agent-re vagy user-re a beszélgetés megnyitásához</div>';
      html += '<div id="chatInputBar" style="padding:12px;border-top:1px solid var(--border);display:none;gap:8px;">';
      html += '<input id="chatFileInput" type="file" style="display:none;" multiple />';
      html += '<button id="chatAttachBtn" title="Fájl csatolása" style="background:var(--surface2);color:var(--text);border:1px solid var(--border);padding:10px 14px;border-radius:8px;cursor:pointer;font-size:16px;">📎</button>';
      html += '<button id="chatDictateBtn" title="Diktálás (beszéd → szöveg)" onclick="toggleDictation(\'chatInput\', this)" style="background:var(--danger);color:#fff;border:none;padding:10px 14px;border-radius:8px;cursor:pointer;font-size:16px;">🎤</button>';
      html += '<input id="chatInput" type="text" placeholder="Üzenet írása... (Enter = küldés)" style="flex:1;background:var(--bg);border:1px solid var(--border);color:var(--text);padding:10px 14px;border-radius:8px;font-size:14px;" />';
      html += '<button id="chatSendBtn" style="background:var(--primary);color:#fff;border:none;padding:10px 20px;border-radius:8px;cursor:pointer;font-size:14px;">➤ Küldés</button>';
      html += '</div>';
      html += '</div>';
      html += '</div>';
      return html;
    },

    'topology': function(d) {
      if (d.error) return errorBox(d.error);
      var nodes = d.nodes || [];
      if (!Array.isArray(nodes) && typeof nodes === 'object') nodes = Object.keys(nodes).map(function(k) { var n = nodes[k] || {}; n.name = n.name || k; return n; });
      var conns = d.connections || [];
      var localNode = d.local_node || '?';
      var html = '';
      // Local node badge
      html += '<div style="background:var(--surface2);border-radius:8px;padding:12px;margin-bottom:16px;border-left:4px solid var(--primary);">';
      html += '<div style="font-size:13px;"><strong>📍 Helyi node:</strong> ' + esc(localNode) + '</div>';
      html += '<div style="font-size:11px;color:var(--text3);margin-top:4px;">Node-ok: ' + nodes.length + ' • Kapcsolatok: ' + conns.length + '</div>';
      html += '</div>';
      // Nodes list
      if (nodes.length) {
        html += '<h3 style="margin:0 0 8px;font-size:13px;">🔗 Node-ok (' + nodes.length + ')</h3>';
        nodes.forEach(function(n) {
          var online = n.status === 'active' || n.status === 'online';
          var dotColor = online ? 'var(--success)' : 'var(--danger)';
          html += card('<div style="display:flex;align-items:center;gap:8px;">' +
            '<div style="width:8px;height:8px;border-radius:50%;background:' + dotColor + ';flex-shrink:0;"></div>' +
            '<strong style="font-size:13px;flex:1;">' + esc(n.name || n.node_name || '?') + '</strong>' +
            '<span style="font-size:10px;color:var(--text3);">' + esc(n.host || '') + ':' + esc(n.port || '') + '</span>' +
            '</div>');
        });
      }
      // Connections
      if (conns.length) {
        html += '<h3 style="margin:16px 0 8px;font-size:13px;">⚡ Kapcsolatok (' + conns.length + ')</h3>';
        conns.forEach(function(c) {
          html += card('<div style="display:flex;align-items:center;gap:6px;font-size:11px;">' +
            '<span style="color:var(--primary);">' + esc(c.from || c.source || '?') + '</span>' +
            '<span style="color:var(--text3);">→</span>' +
            '<span style="color:var(--primary);">' + esc(c.to || c.target || '?') + '</span>' +
            '<span style="margin-left:auto;color:var(--text3);">' + esc(c.type || c.transport || '') + '</span>' +
            '</div>');
        });
      }
      return html || empty('Nincs topológia adat');
    },
    'governance': function(d) {
      if (d.error) return errorBox(d.error);
      var rules = d.rules || [];
      var html = '';
      if (!rules.length) return empty('Nincs governance szabály');
      html += '<div style="display:flex;flex-direction:column;gap:8px;">';
      rules.forEach(function(r) {
        var enabled = r.enabled !== false;
        var enColor = enabled ? 'var(--success)' : 'var(--text3)';
        html += card('<div style="display:flex;align-items:flex-start;gap:8px;">' +
          '<span style="font-size:9px;padding:2px 6px;border-radius:4px;background:' + enColor + '22;color:' + enColor + ';font-weight:600;flex-shrink:0;">' + (enabled ? 'ON' : 'OFF') + '</span>' +
          '<div style="flex:1;"><strong style="font-size:12px;">' + esc(r.name || r.id || '—') + '</strong>' +
          '<div style="font-size:10px;color:var(--text3);margin-top:2px;">' + esc(r.description || '') + '</div>' +
          '<div style="font-size:10px;color:var(--text3);margin-top:2px;">Művelet: <strong style="color:var(--text2);">' + esc(r.action || '—') + '</strong></div>' +
          '</div></div>');
      });
      html += '</div>';
      return html;
    },
    'watchers': function(d) {
      if (d.error) return errorBox(d.error);
      var issues = d.issues || [];
      var html = '';
      // Stats
      var issueCount = issues.length;
      var statColor = issueCount === 0 ? 'var(--success)' : 'var(--warning)';
      html += '<div style="background:var(--surface2);border-radius:8px;padding:12px;margin-bottom:16px;border-left:4px solid ' + statColor + ';">';
      html += '<div style="font-size:14px;font-weight:600;color:' + statColor + ';">' + (issueCount === 0 ? '✅ Nincs elakadt feladat' : '⚠️ ' + issueCount + ' elakadt feladat') + '</div>';
      html += '</div>';
      if (!issues.length) return html;
      html += '<div style="display:flex;flex-direction:column;gap:8px;">';
      issues.forEach(function(i) {
        var sevColor = i.severity === 'critical' ? 'var(--danger)' : i.severity === 'high' ? 'var(--warning)' : 'var(--text3)';
        html += card('<div style="display:flex;align-items:flex-start;gap:8px;">' +
          '<span style="font-size:9px;padding:2px 6px;border-radius:4px;background:' + sevColor + ';color:#fff;font-weight:600;flex-shrink:0;">' + esc(i.severity || 'medium') + '</span>' +
          '<div style="flex:1;"><strong style="font-size:12px;">' + esc(i.title || i.task_id || '—') + '</strong>' +
          '<div style="font-size:10px;color:var(--text3);margin-top:2px;">' + esc(i.description || i.reason || '') + '</div>' +
          '<div style="font-size:10px;color:var(--text3);margin-top:2px;">Agent: ' + esc(i.agent || '?') + ' • Vár: ' + esc(i.waiting_for || '') + '</div>' +
          '</div></div>');
      });
      html += '</div>';
      return html;
    },
    'routing': function(d) {
      if (d.error) return errorBox(d.error);
      var sent = d.sent || 0;
      var received = d.received || 0;
      var forwarded = d.forwarded || 0;
      var duplicates = d.duplicates || 0;
      var errors = d.errors || 0;
      var dedup = d.dedup || {};
      var html = '';
      // Router stats
      html += '<div style="display:flex;gap:8px;margin-bottom:16px;flex-wrap:wrap;">';
      var stats = [
        {label: 'Küldött', val: sent, color: 'var(--primary)'},
        {label: 'Fogadott', val: received, color: 'var(--success)'},
        {label: 'Továbbítva', val: forwarded, color: 'var(--warning)'},
        {label: 'Duplikátum', val: duplicates, color: 'var(--text3)'},
        {label: 'Hibák', val: errors, color: 'var(--danger)'}
      ];
      stats.forEach(function(s) {
        html += '<div style="background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:8px 14px;text-align:center;min-width:80px;"><div style="font-size:18px;font-weight:700;color:' + s.color + ';">' + esc(s.val) + '</div><div style="font-size:10px;color:var(--text3);">' + esc(s.label) + '</div></div>';
      });
      html += '</div>';
      // Protocol info
      html += '<div style="background:var(--surface2);border-radius:8px;padding:12px;margin-bottom:16px;">';
      html += '<div style="font-size:11px;color:var(--text3);margin-bottom:4px;">Protokoll: <strong style="color:var(--text);">' + esc(d.protocol_version || '?') + '</strong></div>';
      var transports = d.transports || {};
      var tKeys = Object.keys(transports);
      var tList = tKeys.map(function(k) { return k + (transports[k] && transports[k].available ? ' ✓' : ' ✗'); }).join(', ');
      html += '<div style="font-size:11px;color:var(--text3);">Transports: <strong style="color:var(--text);">' + esc(tList) + '</strong></div>';
      if (d.degraded_mode) {
        html += '<div style="font-size:11px;color:var(--warning);margin-top:4px;">⚠️ Degradált mód aktív</div>';
      }
      html += '</div>';
      // Circuit breakers
      if (d.circuit_breakers && (Array.isArray(d.circuit_breakers) ? d.circuit_breakers.length : Object.keys(d.circuit_breakers).length)) {
        var cbs = Array.isArray(d.circuit_breakers) ? d.circuit_breakers : Object.keys(d.circuit_breakers).map(function(k) { var cb = d.circuit_breakers[k] || {}; cb.name = cb.name || k; return cb; });
        html += '<h3 style="margin:0 0 8px;font-size:13px;">⚡ Circuit Breakerek</h3>';
        cbs.forEach(function(cb) {
          var cbColor = cb.state === 'closed' ? 'var(--success)' : cb.state === 'open' ? 'var(--danger)' : 'var(--warning)';
          html += card('<div style="display:flex;align-items:center;gap:8px;"><span style="font-size:9px;padding:2px 6px;border-radius:4px;background:' + cbColor + '22;color:' + cbColor + ';">' + esc(cb.state || '?') + '</span><strong style="font-size:12px;flex:1;">' + esc(cb.name || cb.target || '?') + '</strong><span style="font-size:10px;color:var(--text3);">Failures: ' + esc(cb.failures || 0) + '</span></div>');
        });
      }
      return html;
    },
    'delegations': function(d) {
      if (d.error) return errorBox(d.error);
      var delegations = d.delegations || [];
      var count = d.count || 0;
      var html = '';
      html += '<div style="display:flex;gap:8px;margin-bottom:16px;flex-wrap:wrap;">';
      html += '<div style="background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:8px 14px;text-align:center;min-width:80px;"><div style="font-size:18px;font-weight:700;color:var(--primary);">' + count + '</div><div style="font-size:10px;color:var(--text3);">Összes</div></div>';
      html += '<div style="background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:8px 14px;text-align:center;min-width:80px;"><div style="font-size:18px;font-weight:700;color:var(--success);">' + delegations.filter(function(x) { return x.status === 'completed'; }).length + '</div><div style="font-size:10px;color:var(--text3);">Kész</div></div>';
      html += '<div style="background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:8px 14px;text-align:center;min-width:80px;"><div style="font-size:18px;font-weight:700;color:var(--warning);">' + delegations.filter(function(x) { return x.status === 'available' || x.status === 'pending'; }).length + '</div><div style="font-size:10px;color:var(--text3);">Függő</div></div>';
      html += '<div style="background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:8px 14px;text-align:center;min-width:80px;"><div style="font-size:18px;font-weight:700;color:var(--danger);">' + delegations.filter(function(x) { return x.status === 'expired' || x.status === 'failed'; }).length + '</div><div style="font-size:10px;color:var(--text3);">Hibás</div></div>';
      html += '</div>';
      if (!delegations.length) return html + empty('Nincs delegáció');
      html += '<div style="display:flex;flex-direction:column;gap:8px;">';
      delegations.slice(0, 30).forEach(function(t) {
        var stColor = t.status === 'completed' ? 'var(--success)' : t.status === 'available' ? 'var(--warning)' : t.status === 'expired' || t.status === 'failed' ? 'var(--danger)' : 'var(--text3)';
        html += card('<div style="display:flex;align-items:flex-start;gap:8px;">' +
          '<span style="font-size:9px;padding:2px 6px;border-radius:4px;background:' + stColor + '22;color:' + stColor + ';font-weight:600;flex-shrink:0;">' + esc(t.status || '?') + '</span>' +
          '<div style="flex:1;"><strong style="font-size:12px;">' + esc(t.subject || t.task_id || '—') + '</strong>' +
          '<div style="font-size:10px;color:var(--text3);margin-top:2px;">' + esc(t.from_agent || '?') + ' → ' + esc(t.to_agent || '?') + ' • ' + fmtTime(t.created_at) + '</div>' +
          (t.description ? '<div style="font-size:10px;color:var(--text3);margin-top:4px;">' + esc((t.description || '').substring(0, 100)) + '</div>' : '') +
          '</div></div>');
      });
      html += '</div>';
      return html;
    },
    'alerts': function(d) {
      if (d.error) return errorBox(d.error);
      var running = d.running;
      var totalRules = d.total_rules || 0;
      var firing = d.firing || 0;
      var rules = d.rules || [];
      var html = '';
      html += '<div style="margin-bottom:12px;"><button onclick="showAlertRuleEditor()" style="background:var(--primary);color:#fff;border:none;padding:6px 12px;border-radius:6px;cursor:pointer;font-size:11px;">➕ Új szabály</button></div>';
      html += '<div style="display:flex;gap:8px;margin-bottom:16px;flex-wrap:wrap;">';
      html += '<div style="background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:8px 14px;text-align:center;min-width:80px;"><div style="font-size:18px;font-weight:700;color:' + (running ? 'var(--success)' : 'var(--danger)') + ';">' + (running ? 'Aktív' : 'Leállt') + '</div><div style="font-size:10px;color:var(--text3);">Státusz</div></div>';
      html += '<div style="background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:8px 14px;text-align:center;min-width:80px;"><div style="font-size:18px;font-weight:700;color:var(--primary);">' + totalRules + '</div><div style="font-size:10px;color:var(--text3);">Szabályok</div></div>';
      html += '<div style="background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:8px 14px;text-align:center;min-width:80px;"><div style="font-size:18px;font-weight:700;color:var(--danger);">' + firing + '</div><div style="font-size:10px;color:var(--text3);">Aktív riasztás</div></div>';
      html += '</div>';
      if (!rules.length) return html + empty('Nincs riasztási szabály');
      html += '<div style="display:flex;flex-direction:column;gap:8px;">';
      rules.forEach(function(r) {
        var enabled = r.enabled !== false;
        var enColor = enabled ? 'var(--success)' : 'var(--text3)';
        html += card('<div style="display:flex;align-items:flex-start;gap:8px;">' +
          '<span style="font-size:9px;padding:2px 6px;border-radius:4px;background:' + enColor + '22;color:' + enColor + ';font-weight:600;flex-shrink:0;">' + (enabled ? 'ON' : 'OFF') + '</span>' +
          '<div style="flex:1;"><strong style="font-size:12px;">' + esc(r.name || r.id || '—') + '</strong>' +
          '<div style="font-size:10px;color:var(--text3);margin-top:2px;">' + esc(r.description || r.condition || '') + '</div>' +
          '<div style="font-size:10px;color:var(--text3);margin-top:2px;">Kategória: ' + esc(r.category || '—') + ' • Prioritás: ' + esc(r.priority || '—') + '</div>' +
          '</div></div>');
      });
      html += '</div>';
      return html;
    },
    'health': function(d) {
      if (d.error) return errorBox(d.error);
      var agents = d.agents || [];
      if (!Array.isArray(agents) && typeof agents === 'object') agents = Object.keys(agents).map(function(k) { var a = agents[k] || {}; a.name = a.name || a.agent || k; return a; });
      var agentCount = d.agent_count || agents.length;
      // Score normalizáció: API 0-1 skálát ad (1.0 = tökéletes), megjelenítés 0-100
      agents.forEach(function(a) { var s = Number(a.score || a.health_score || 0); a._score = (s > 0 && s <= 1) ? Math.round(s * 100) : Math.round(s); });
      var avgScore = agents.length ? Math.round(agents.reduce(function(t, a) { return t + a._score; }, 0) / agents.length) : 0;
      var totReq = agents.reduce(function(t, a) { return t + (a.requests || 0); }, 0);
      var totSucc = agents.reduce(function(t, a) { return t + (a.successes || 0); }, 0);
      var totFail = agents.reduce(function(t, a) { return t + (a.failures || 0); }, 0);
      var avgLat = agents.length ? Math.round(agents.reduce(function(t, a) { return t + (a.avg_latency_ms || 0); }, 0) / agents.length) : 0;
      var totDeleg = agents.reduce(function(t, a) { return t + (a.active_delegations || 0); }, 0);
      var failRate = totReq ? Math.round(totFail / totReq * 100) : 0;
      var avgColor = avgScore >= 80 ? 'var(--success)' : avgScore >= 50 ? 'var(--warning)' : 'var(--danger)';
      var html = '';
      // ── Összesítő stat kártyák ──
      html += '<div style="display:flex;gap:8px;margin-bottom:14px;flex-wrap:wrap;">';
      var stats = [
        {l: 'Agentek', v: agentCount, c: 'var(--primary)'},
        {l: 'Átlag score', v: avgScore + '%', c: avgColor},
        {l: 'Kérések', v: totReq, c: 'var(--text)'},
        {l: 'Sikeres', v: totSucc, c: 'var(--success)'},
        {l: 'Hibás', v: totFail + ' (' + failRate + '%)', c: totFail ? 'var(--danger)' : 'var(--success)'},
        {l: 'Átlag latency', v: avgLat + 'ms', c: 'var(--warning)'},
        {l: 'Aktív delegek', v: totDeleg, c: '#c084fc'}
      ];
      stats.forEach(function(s) {
        html += '<div style="background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:8px 12px;text-align:center;min-width:74px;flex:1;"><div style="font-size:16px;font-weight:700;color:' + s.c + ';">' + esc(s.v) + '</div><div style="font-size:9px;color:var(--text3);margin-top:2px;">' + esc(s.l) + '</div></div>';
      });
      html += '</div>';
      if (!agents.length) return html + empty('Nincs egészség adat');
      // ── Agent kártyák ──
      html += '<h3 style="margin:0 0 8px;font-size:13px;color:var(--text2);">🩺 Agent állapot</h3>';
      html += '<div style="display:flex;flex-direction:column;gap:8px;">';
      agents.forEach(function(a) {
        var score = a._score;
        var scoreColor = score >= 80 ? 'var(--success)' : score >= 50 ? 'var(--warning)' : 'var(--danger)';
        var stateColor = a.agent_state === 'idle' ? 'var(--success)' : a.agent_state === 'busy' ? 'var(--warning)' : 'var(--danger)';
        html += card('<div style="display:flex;align-items:center;gap:10px;">' +
          '<div style="width:44px;height:44px;border-radius:50%;background:' + scoreColor + ';display:flex;align-items:center;justify-content:center;font-size:14px;font-weight:700;color:#fff;flex-shrink:0;">' + score + '</div>' +
          '<div style="flex:1;min-width:0;">' +
            '<div style="display:flex;align-items:center;gap:6px;flex-wrap:wrap;"><strong style="font-size:13px;">' + esc(a.name || '—') + '</strong>' +
            badge(a.agent_state || 'ismeretlen', stateColor) +
            (a.consecutive_failures > 0 ? badge('⚠ ' + a.consecutive_failures + ' sorozathiba', 'var(--danger)') : '') +
            (a.active_delegations > 0 ? badge('📋 ' + a.active_delegations + ' delegálás', '#c084fc') : '') +
            '</div>' +
            '<div style="display:flex;gap:10px;margin-top:4px;font-size:10px;color:var(--text3);flex-wrap:wrap;">' +
              '<span>📤 ' + esc(a.requests || 0) + ' kérés</span>' +
              '<span style="color:var(--success);">✅ ' + esc(a.successes || 0) + '</span>' +
              '<span style="color:' + (a.failures ? 'var(--danger)' : 'var(--text3)') + ';">❌ ' + esc(a.failures || 0) + '</span>' +
              '<span>⚡ ' + esc(a.avg_latency_ms || 0) + 'ms</span>' +
            '</div>' +
            '<div style="display:flex;gap:4px;margin-top:4px;font-size:9px;flex-wrap:wrap;">' +
              '<span style="padding:1px 6px;border-radius:8px;background:var(--surface2);color:var(--text3);">Primary: ' + esc(a.provider_primary || '?') + '</span>' +
              '<span style="padding:1px 6px;border-radius:8px;background:var(--surface2);color:var(--text3);">Fallback: ' + esc(a.provider_fallback || '?') + '</span>' +
            '</div>' +
          '</div></div>');
      });
      html += '</div>';
      // ── Extras placeholder-ek (async betöltés) ──
      html += '<div id="health-nodes-section" style="margin-top:12px;"></div>';
      html += '<div id="health-p2p-section" style="margin-top:12px;"></div>';
      return html;
    },
    'diagnostics': function(d) {
      if (d.error) return errorBox(d.error);
      var status = d.status || {};
      var reports = d.recent_reports || [];
      var suggestions = d.recent_suggestions || [];
      var html = '';
      // Status bar
      html += '<div style="display:flex;gap:8px;margin-bottom:16px;flex-wrap:wrap;">';
      var statItems = [
        {label: 'Engedélyezett', val: status.enabled ? 'Igen' : 'Nem', color: status.enabled ? 'var(--success)' : 'var(--danger)'},
        {label: 'Reportok', val: status.reports_stored || 0, color: 'var(--primary)'},
        {label: 'Javaslatok', val: status.suggestions_stored || 0, color: 'var(--warning)'},
        {label: 'Developer node', val: (status.developer_nodes || []).join(', ') || 'nincs', color: '#c084fc'}
      ];
      statItems.forEach(function(s) {
        html += '<div style="background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:8px 14px;text-align:center;min-width:100px;">' +
          '<div style="font-size:16px;font-weight:700;color:' + s.color + ';">' + esc(s.val) + '</div>' +
          '<div style="font-size:10px;color:var(--text3);margin-top:2px;">' + esc(s.label) + '</div></div>';
      });
      html += '</div>';
      // Action buttons
      html += '<div style="display:flex;gap:8px;margin-bottom:16px;">';
      html += '<button onclick="generateDiagnosticReport()" style="background:var(--primary);color:#fff;border:none;padding:8px 16px;border-radius:8px;cursor:pointer;font-size:12px;">🔧 Jelentés generálása</button>';
      html += '<button onclick="autoImplementSuggestions()" style="background:var(--success);color:#fff;border:none;padding:8px 16px;border-radius:8px;cursor:pointer;font-size:12px;">⚡ Auto-implement</button>';
      html += '<button onclick="importDiagnosticsAsIdeas()" style="background:var(--warning);color:#000;border:none;padding:8px 16px;border-radius:8px;cursor:pointer;font-size:12px;">💡 Import ötletládába</button>';
      html += '</div>';
      // Reports section
      if (reports.length) {
        html += '<h3 style="margin:0 0 10px;font-size:14px;">📋 Jelentések (' + reports.length + ')</h3>';
        reports.forEach(function(r) {
          var sevColor = r.severity === 'critical' ? 'var(--danger)' : r.severity === 'warning' ? 'var(--warning)' : 'var(--text3)';
          html += card('<div style="display:flex;align-items:center;gap:8px;margin-bottom:6px;">' +
            '<span style="font-size:9px;padding:2px 6px;border-radius:4px;background:' + sevColor + ';color:#fff;font-weight:600;">' + esc(r.severity || 'info') + '</span>' +
            '<strong style="font-size:13px;flex:1;">' + esc(r.node || '?') + ' — ' + esc(r.report_type || 'on_demand') + '</strong>' +
            '<span style="font-size:10px;color:var(--text3);">' + fmtTime(r.timestamp) + '</span></div>' +
            '<div style="font-size:11px;color:var(--text2);margin-bottom:6px;">' + esc(r.summary || '') + '</div>');
        });
      }
      // Suggestions section
      if (suggestions.length) {
        html += '<h3 style="margin:20px 0 10px;font-size:14px;">💡 Javaslatok (' + suggestions.length + ')</h3>';
        suggestions.slice(0, 20).forEach(function(s) {
          var priColor = s.priority === 'critical' ? 'var(--danger)' : s.priority === 'high' ? 'var(--warning)' : s.priority === 'medium' ? 'var(--primary)' : 'var(--text3)';
          var stColor = s.status === 'accepted' ? 'var(--success)' : s.status === 'rejected' ? 'var(--danger)' : 'var(--text3)';
          html += card('<div style="display:flex;align-items:flex-start;gap:8px;margin-bottom:6px;">' +
            '<span style="font-size:9px;padding:2px 6px;border-radius:4px;background:' + priColor + ';color:#fff;font-weight:600;flex-shrink:0;">' + esc(s.priority) + '</span>' +
            '<div style="flex:1;"><strong style="font-size:13px;">' + esc(s.title || '') + '</strong>' +
            '<div style="font-size:11px;color:var(--text3);margin-top:4px;">' + esc(s.description || '') + '</div>' +
            '<div style="display:flex;gap:6px;margin-top:6px;align-items:center;">' +
            '<span style="font-size:9px;color:var(--text3);">📂 ' + esc(s.category || '') + '</span>' +
            '<span style="font-size:9px;color:var(--text3);">📍 ' + esc(s.node || '') + '</span>' +
            '<span style="font-size:9px;padding:1px 6px;border-radius:4px;background:' + stColor + '22;color:' + stColor + ';">' + esc(s.status || 'pending') + '</span>' +
            '</div></div></div>');
        });
        if (suggestions.length > 20) {
          html += '<div style="text-align:center;color:var(--text3);font-size:11px;padding:8px;">+ ' + (suggestions.length - 20) + ' további javaslat...</div>';
        }
      }
      return html || empty('Nincs diagnosztikai adat');
    },
    'insights-dream': function(d) {
      if (d.error) return errorBox(d.error);
      var html = '<div style="padding:16px;">';
      
      // Header + trigger button
      html += '<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:16px;">';
      html += '<div><h2 style="margin:0;font-size:18px;">🌙 Dream Engine</h2>';
      html += '<div style="font-size:12px;color:var(--text3);margin-top:2px;">Fut minden ' + (d.interval_hours || 6) + ' órában</div></div>';
      html += '<button onclick="triggerDream()" id="dreamTriggerBtn" style="background:var(--primary);color:#fff;border:none;padding:10px 20px;border-radius:8px;cursor:pointer;font-size:13px;">▶ Indítás most</button>';
      html += '</div>';
      
      // Status card
      var enabledColor = d.enabled ? 'var(--success)' : 'var(--danger)';
      html += '<div style="background:var(--surface2);border-radius:8px;padding:14px;margin-bottom:16px;border-left:4px solid ' + enabledColor + ';">';
      html += '<div style="display:flex;align-items:center;gap:8px;margin-bottom:8px;">';
      html += '<div style="width:10px;height:10px;border-radius:50%;background:' + enabledColor + ';"></div>';
      html += '<strong style="font-size:14px;">' + (d.enabled ? 'Aktív' : 'Inaktív') + '</strong>';
      html += '</div>';
      html += '<div style="display:grid;grid-template-columns:repeat(3,1fr);gap:8px;">';
      html += '<div style="text-align:center;"><div style="font-size:20px;font-weight:700;color:var(--primary);">' + (d.total_dreams || 0) + '</div><div style="font-size:10px;color:var(--text3);">Összes álom</div></div>';
      html += '<div style="text-align:center;"><div style="font-size:13px;font-weight:600;">' + fmtTime(d.last_run) + '</div><div style="font-size:10px;color:var(--text3);">Utolsó futás</div></div>';
      html += '<div style="text-align:center;"><div style="font-size:13px;font-weight:600;">' + fmtTime(d.next_run) + '</div><div style="font-size:10px;color:var(--text3);">Következő</div></div>';
      html += '</div></div>';

      // Recent results
      var results = d.recent_results || [];
      if (results.length) {
        html += '<h3 style="margin:0 0 10px;font-size:14px;color:var(--text3);text-transform:uppercase;letter-spacing:.5px;">Legutóbbi eredmények</h3>';
        results.forEach(function(r) {
          html += '<div style="background:var(--surface2);border-radius:8px;padding:12px;margin-bottom:8px;border:1px solid var(--border);">';
          html += '<div style="font-size:12px;color:var(--text2);line-height:1.4;">' + esc(r.preview) + '</div>';
          html += '<div style="font-size:10px;color:var(--text3);margin-top:4px;">' + fmtTime(r.timestamp) + '</div>';
          html += '</div>';
        });
      } else {
        html += '<div style="text-align:center;padding:20px;color:var(--text3);font-size:13px;">Még nincs álom eredmény. Kattints az "Indítás most" gombra.</div>';
      }
      
      html += '</div>';
      return html;
    },
    'insights-conversations': function(d) {
      // This is called when no API path — show agent selector
      var agents = ['nova', 'morzsa', 'runa'];
      var html = '<div style="margin-bottom:16px;">' +
        '<label style="display:block;font-size:12px;color:var(--text3);margin-bottom:6px;">Válassz agentet:</label>' +
        '<select id="convAgentSelect" onchange="loadConversationLog()" style="width:100%;padding:8px;border-radius:6px;background:var(--surface2);border:1px solid var(--border);color:var(--text);font-size:13px;">' +
        '<option value="">-- válassz --</option>';
      agents.forEach(function(a) {
        html += '<option value="' + a + '">' + a + '</option>';
      });
      html += '</select></div>';
      html += '<div id="convLogContainer">' + empty('Válassz agentet a beszélgetések megtekintéséhez') + '</div>';
      return html;
    }
  };

  if (apiPath && apiPath !== 'none') {
    fetch(apiPath, { headers: { 'Authorization': 'Bearer ' + token } })
      .then(function(r) {
        if (r.status === 401) {
          // Token expired — try re-login
          console.log('[DASH] Token expired (401), re-login needed');
          body.innerHTML = '<div style="text-align:center;padding:40px;color:var(--danger)"><div style="font-size:32px;margin-bottom:12px">⚠️</div><p>A munkamenet lejárt.</p><button onclick="doLogout();setTimeout(function(){location.reload();},100);" style="background:var(--primary);color:#fff;border:none;padding:10px 24px;border-radius:8px;cursor:pointer;margin-top:12px;">Újra bejelentkezés</button></div>';
          throw new Error('401');
        }
        if (!r.ok) throw new Error('HTTP ' + r.status);
        return r.json();
      })
      .then(function(data) {
        var renderer = renderers[page];
        if (renderer) {
          try { body.innerHTML = renderer(data); }
          catch(e) { body.innerHTML = errorBox('Render hiba: ' + e.message); }
          // Chat: attach click handlers to contact items + send button + input
          if (page === 'chat') {
            var items = document.querySelectorAll('.chat-contact-item');
            for (var i = 0; i < items.length; i++) {
              items[i].onclick = function() {
                var agentName = this.getAttribute('data-chat-agent');
                if (agentName) selectChatContact(agentName);
              };
            }
            var sendBtn = document.getElementById('chatSendBtn');
            if (sendBtn) sendBtn.onclick = function() { sendChatMessage(); };
            var chatInput = document.getElementById('chatInput');
            if (chatInput) {
              chatInput.onkeyup = function(e) {
                if (e.key === 'Enter' && document.getElementById('commandPalette')) return;
                if (e.key === 'Enter') sendChatMessage();
              };
            }
          }
        } else {
          body.innerHTML = '<pre style="white-space:pre-wrap;font-size:12px;color:var(--text2);">' + esc(JSON.stringify(data, null, 2)) + '</pre>';
        }
      })
      .catch(function(e) {
        body.innerHTML = errorBox(e.message);
      });
  } else {
    // No API path — check if there's a renderer that doesn't need fetch
    var renderer = renderers[page];
    if (renderer) {
      try { body.innerHTML = renderer({}); }
      catch(e) { body.innerHTML = errorBox('Render hiba: ' + e.message); }
    } else {
      body.innerHTML = '<div style="text-align:center;padding:60px;">' +
        '<div style="font-size:48px;margin-bottom:20px;">⏳</div>' +
        '<p style="color:var(--text3);font-size:16px;max-width:500px;margin:0 auto;">Ez a funkció fejlesztés alatt áll.</p>' +
        '</div>';
    }
  }
}

function closeMarveenModal() {
  var modal = document.getElementById('marveenModal');
  if (modal) modal.style.display = 'none';
}

// ═══════════════════════════════════════════════════════════
// ── Diktálás (Web Speech API) — beszéd → szöveg minden chatbe ──
// ═══════════════════════════════════════════════════════════
window._dictation = { active: false, inputId: null, btn: null, recog: null, lang: 'hu-HU' };

window.toggleDictation = function(inputId, btn) {
  var st = window._dictation;
  // Aktív diktálás leállítása (ugyanaz a gomb vagy másik)
  if (st.active) {
    if (st.recog) { try { st.recog.stop(); } catch (e) {} }
    st.active = false;
    if (st.btn) { st.btn.textContent = '🎤'; st.btn.style.background = 'var(--danger)'; st.btn.title = 'Diktálás (beszéd → szöveg)'; }
    var inp = document.getElementById(st.inputId);
    if (inp) inp.placeholder = inp.placeholder.replace(' 🎙️ Diktálsz...', '');
    st.recog = null; st.btn = null;
    if (st.inputId === inputId) { st.inputId = null; return; } // ugyanaz → csak leállítás
  }
  // Új diktálás indítása
  var SR = window.SpeechRecognition || window.webkitSpeechRecognition;
  if (!SR) {
    showToast('🎤 A böngésző nem támogatja a diktálást (Safari/Chrome ajánlott)', 'error');
    return;
  }
  var input = document.getElementById(inputId);
  if (!input) return;
  var recog = new SR();
  recog.lang = st.lang;
  recog.continuous = true;      // hosszú diktálás — pihentetés nélkül
  recog.interimResults = true;  // élő szöveg az inputban
  recog.maxAlternatives = 1;
  var baseText = input.value ? input.value + ' ' : '';
  var finalText = baseText;
  st.recog = recog; st.active = true; st.inputId = inputId; st.btn = btn;
  if (btn) { btn.textContent = '🎙️'; btn.style.background = 'var(--warning, #d29922)'; btn.title = 'Diktálás aktív — kattints a leállításhoz'; }
  if (input) input.placeholder = input.placeholder + ' 🎙️ Diktálsz...';

  recog.onresult = function(ev) {
    var input = document.getElementById(inputId);
    if (!input) return;
    var interim = '';
    for (var i = ev.resultIndex; i < ev.results.length; i++) {
      if (ev.results[i].isFinal) {
        finalText += ev.results[i][0].transcript;
      } else {
        interim += ev.results[i][0].transcript;
      }
    }
    input.value = (finalText + interim).trim();
    // Kurzor a végére
    input.setSelectionRange(input.value.length, input.value.length);
  };
  recog.onerror = function(ev) {
    if (ev.error === 'not-allowed' || ev.error === 'service-not-allowed') {
      showToast('🎤 Mikrofon-hozzáférés megtagadva — engedélyezd a böngészőben', 'error');
      window.toggleDictation(inputId, btn); // leállítás
    } else if (ev.error !== 'no-speech' && ev.error !== 'aborted') {
      showToast('🎤 Diktálás hiba: ' + ev.error, 'warning');
    }
  };
  recog.onend = function() {
    // Automatikus újraindítás, ha még aktív (a Chrome 60s után leállítja)
    if (window._dictation.active && window._dictation.inputId === inputId) {
      try { window._dictation.recog.start(); } catch (e) {}
    } else {
      var b = window._dictation.btn;
      if (b) { b.textContent = '🎤'; b.style.background = 'var(--danger)'; }
      var inp = document.getElementById(inputId);
      if (inp) inp.placeholder = inp.placeholder.replace(' 🎙️ Diktálsz...', '');
      window._dictation.active = false; window._dictation.recog = null; window._dictation.btn = null;
    }
  };
  try {
    recog.start();
    showToast('🎤 Diktálás aktív — beszélj, a szöveg automatikusan beíródik', 'success');
  } catch (e) {
    showToast('🎤 Diktálás indítása sikertelen: ' + e.message, 'error');
    st.active = false; st.recog = null;
  }
};

// Nyelv váltása a diktáláshoz (ha kell: toggleDictationLang('en-US'))
window.toggleDictationLang = function(lang) {
  window._dictation.lang = lang || (window._dictation.lang === 'hu-HU' ? 'en-US' : 'hu-HU');
  showToast('🎤 Diktálás nyelv: ' + window._dictation.lang, 'info');
  return window._dictation.lang;
};

// ── Dokumentáció megtekintő — A2A Mesh docs fájl tartalom modal ──
window.viewDocFile = function(name, source) {
  var token = localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || '';
  // Modal megnyitása betöltés-jelzéssel
  var modal = document.getElementById('marveenModal');
  if (modal) {
    document.getElementById('marveenModalTitle').textContent = '📄 ' + name;
    document.getElementById('marveenModalBody').innerHTML = '<div style="text-align:center;padding:40px;color:var(--text3);">⏳ Betöltés...</div>';
    modal.style.display = 'flex';
  }
  fetch('/api/docs/content?name=' + encodeURIComponent(name) + '&source=' + encodeURIComponent(source), {
    headers: { 'Authorization': 'Bearer ' + token }
  })
  .then(function(r) { if (!r.ok) throw new Error('HTTP ' + r.status); return r.json(); })
  .then(function(d) {
    var body = document.getElementById('marveenModalBody');
    if (!body) return;
    if (d.error) {
      body.innerHTML = '<div style="padding:20px;color:var(--danger);">❌ ' + esc(d.error) + '</div>';
      return;
    }
    body.innerHTML = renderDocContent(d.content || '', name);
  })
  .catch(function(e) {
    var body = document.getElementById('marveenModalBody');
    if (body) body.innerHTML = '<div style="padding:20px;color:var(--danger);">❌ Betöltés sikertelen: ' + esc(String(e.message || e)) + '</div>';
  });
};

// Egyszerű markdown → HTML render (fejlécek, listák, kód, vastag/dőlt, linkek)
function renderDocContent(md, name) {
  var lines = String(md || '').split('\n');
  var out = '';
  var inCode = false, codeBuf = [];
  var inList = false;
  function closeList() { if (inList) { out += '</ul>'; inList = false; } }
  function inline(s) {
    return esc(s)
      .replace(/`([^`]+)`/g, '<code style="background:var(--surface2);padding:1px 5px;border-radius:4px;font-size:12px;">$1</code>')
      .replace(/\*\*([^*]+)\*\*/g, '<strong>$1</strong>')
      .replace(/\*([^*]+)\*/g, '<em>$1</em>')
      .replace(/\[([^\]]+)\]\(([^)]+)\)/g, '<a href="$2" target="_blank" style="color:var(--primary);">$1</a>');
  }
  for (var i = 0; i < lines.length; i++) {
    var line = lines[i];
    if (line.indexOf('```') === 0) {
      if (inCode) {
        out += '<pre style="background:var(--surface2);padding:12px;border-radius:8px;overflow-x:auto;font-size:12px;margin:8px 0;"><code>' + esc(codeBuf.join('\n')) + '</code></pre>';
        codeBuf = []; inCode = false;
      } else { closeList(); inCode = true; }
      continue;
    }
    if (inCode) { codeBuf.push(line); continue; }
    var t = line.trim();
    if (!t) { closeList(); continue; }
    var h = t.match(/^(#{1,4})\s+(.*)$/);
    if (h) {
      closeList();
      var lvl = Math.min(4, h[1].length + 1);
      out += '<h' + lvl + ' style="margin:14px 0 6px;font-size:' + (20 - lvl * 2) + 'px;color:var(--text);">' + inline(h[2]) + '</h' + lvl + '>';
      continue;
    }
    if (/^[-*]\s+/.test(t) || /^\d+\.\s+/.test(t)) {
      if (!inList) { out += '<ul style="margin:6px 0;padding-left:20px;">'; inList = true; }
      out += '<li style="margin:3px 0;">' + inline(t.replace(/^[-*]\s+/, '').replace(/^\d+\.\s+/, '')) + '</li>';
      continue;
    }
    closeList();
    out += '<p style="margin:6px 0;line-height:1.6;">' + inline(t) + '</p>';
  }
  closeList();
  if (inCode && codeBuf.length) {
    out += '<pre style="background:var(--surface2);padding:12px;border-radius:8px;overflow-x:auto;font-size:12px;margin:8px 0;"><code>' + esc(codeBuf.join('\n')) + '</code></pre>';
  }
  return '<div style="font-size:13px;max-width:860px;margin:0 auto;">' + out + '</div>';
}

// Load conversation log for selected agent
window.loadConversationLog = function() {
  var sel = document.getElementById('convAgentSelect');
  if (!sel || !sel.value) return;
  var agent = sel.value;
  var container = document.getElementById('convLogContainer');
  if (!container) return;
  container.innerHTML = '<div style="text-align:center;padding:20px;color:var(--text3);">⏳ Betöltés...</div>';
  var token = localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || '';
  fetch('/api/insights/conversations/' + encodeURIComponent(agent) + '?limit=50', {
    headers: { 'Authorization': 'Bearer ' + token }
  })
    .then(function(r) { if (!r.ok) throw new Error('HTTP ' + r.status); return r.json(); })
    .then(function(d) {
      var msgs = d.messages || [];
      if (!msgs.length) {
        container.innerHTML = '<div style="text-align:center;padding:40px;color:var(--text3);font-size:14px;">Nincs beszélgetés</div>';
        return;
      }
      var html = '<div style="display:flex;flex-direction:column;gap:8px;padding:10px;max-height:600px;overflow-y:auto;">';
      msgs.forEach(function(m) {
        var isUser = m.role === 'user';
        var align = isUser ? 'flex-start' : 'flex-end';
        var bg = isUser ? 'var(--primary)' : 'var(--success)';
        html += '<div style="display:flex;justify-content:' + align + ';">' +
          '<div style="max-width:80%;padding:8px 12px;border-radius:12px;background:' + bg + ';color:#fff;font-size:13px;line-height:1.4;word-wrap:break-word;">' +
          '<div style="font-size:10px;opacity:.8;margin-bottom:4px;font-weight:600;">' + (isUser ? 'Felhasználó' : 'Asszisztens') + '</div>' +
          '<div>' + (typeof m.content === 'string' ? m.content.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;') : '') + '</div>' +
          '<div style="font-size:10px;opacity:.6;margin-top:4px;text-align:right;">' + (m.timestamp || '') + '</div>' +
          '</div></div>';
      });
      html += '</div>';
      container.innerHTML = html;
    })
    .catch(function(e) {
      container.innerHTML = '<div style="color:var(--danger);text-align:center;padding:20px;">Hiba: ' + e.message + '</div>';
    });
};

// ─── Ideas Board (Ötletláda) Functions ──────────────────────
// ─── Skills Marketplace Functions ───────────────────────────
// ─── Dream Engine Functions ─────────────────────────────────
window.triggerDream = function() {
  var btn = document.getElementById('dreamTriggerBtn');
  if (btn) { btn.disabled = true; btn.textContent = '⏳ Fut...'; }
  var token = localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || '';
  fetch('/api/insights/dream/trigger', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + token }
  }).then(function(r) { return r.json(); })
    .then(function(d) {
      if (btn) { btn.disabled = false; btn.textContent = '▶ Indítás most'; }
      if (d.ok) {
        alert('✅ Dream cycle lefutott!\nBuckets: ' + (d.buckets || []).join(', '));
        loadMarveenPage('insights-dream');
      } else {
        alert('Hiba: ' + (d.error || 'ismeretlen'));
      }
    })
    .catch(function(e) {
      if (btn) { btn.disabled = false; btn.textContent = '▶ Indítás most'; }
      alert('Hiba: ' + e.message);
    });
};
function _collectSkillTags(skills) {
  var tags = {};
  skills.forEach(function(s) {
    (s.tags || []).forEach(function(t) { tags[t] = true; });
  });
  return Object.keys(tags).sort();
}

window.filterSkills = function() {
  var query = '';
  var el = document.getElementById('skillSearch');
  if (el) query = el.value.toLowerCase().trim();
  var agentF = '';
  var elA = document.getElementById('skillFilterAgent');
  if (elA) agentF = elA.value.toLowerCase();
  var tagF = '';
  var elT = document.getElementById('skillFilterTag');
  if (elT) tagF = elT.value.toLowerCase();
  var cards = document.querySelectorAll('.skill-card');
  var visible = 0;
  cards.forEach(function(c) {
    var name = c.getAttribute('data-name') || '';
    var agent = c.getAttribute('data-agent') || '';
    var tags = c.getAttribute('data-tags') || '';
    var match = true;
    if (query && name.indexOf(query) < 0 && tags.indexOf(query) < 0) match = false;
    if (agentF && agent.indexOf(agentF) < 0) match = false;
    if (tagF && tags.indexOf(tagF) < 0) match = false;
    c.style.display = match ? '' : 'none';
    if (match) visible++;
  });
  var empty = document.getElementById('skillsEmpty');
  if (empty) empty.style.display = visible === 0 ? 'block' : 'none';
};

// ==================== CHAT FUNCTIONS ====================

window._chatActiveContact = null;
window._chatPollTimer = null;

window.selectChatContact = function(agentName) {
  window._chatActiveContact = agentName;
  var token = localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || '';
  console.log('[CHAT] selectChatContact:', agentName, 'token:', token ? 'yes' : 'NO');
  // Update header
  var header = document.getElementById('chatHeader');
  if (header) header.textContent = (agentName.indexOf('user:') === 0 ? '👤' : '🤖') + ' ' + agentName;
  // Show input bar
  var inputBar = document.getElementById('chatInputBar');
  if (inputBar) inputBar.style.display = 'flex';
  // Attach send button + input handlers (in case they weren't attached)
  var sendBtn = document.getElementById('chatSendBtn');
  if (sendBtn) {
    sendBtn.onclick = function() { window.sendChatMessage(); };
    // ── File attach: hidden input + upload to current chat channel ──
    var attachBtn = document.getElementById('chatAttachBtn');
    var fileInput = document.getElementById('chatFileInput');
    if (attachBtn && fileInput) {
      attachBtn.onclick = function() { fileInput.click(); };
      fileInput.onchange = function() {
        var files = this.files;
        if (!files || !files.length) return;
        var token = localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || '';
        var ch = window._chatActiveContact || 'broadcast';
        var recipient = (ch === 'general' || ch === 'broadcast') ? '' : ch;
        var fd = new FormData();
        for (var i = 0; i < files.length; i++) fd.append('file', files[i]);
        fd.append('recipient', recipient);
        fd.append('message', 'Fájl megosztva a chatben');
        attachBtn.textContent = '⏳';
        fetch('/api/send-file', {
          method: 'POST',
          headers: { 'Authorization': 'Bearer ' + token },
          body: fd
        }).then(function(r) {
          return r.text().then(function(t) { try { return JSON.parse(t); } catch(e) { throw new Error("Szerver válasz: " + r.status + " " + t.substring(0, 120)); } });
        }).then(function(d) {
          attachBtn.textContent = '📎';
          if (d.ok || d.status === 'ok' || d.file_name || d.filename) {
            if (typeof window._loadChatMessages === 'function') window._loadChatMessages(ch === 'general' ? 'broadcast' : ch, true);
            if (typeof loadMessages === 'function') loadMessages();
          } else {
            alert('Fájl feltöltés hiba: ' + (d.error || 'ismeretlen'));
          }
          fileInput.value = '';
        }).catch(function(e) {
          attachBtn.textContent = '📎';
          alert('Fájl feltöltés hiba: ' + e.message);
          fileInput.value = '';
        });
      };
    }
    console.log('[CHAT] send button handler attached');
  } else {
    console.warn('[CHAT] chatSendBtn NOT FOUND');
  }
  var chatInput = document.getElementById('chatInput');
  if (chatInput) {
    chatInput.onkeyup = function(e) {
      if ((e.key === 'Enter' || e.keyCode === 13) && document.getElementById('commandPalette')) return;
      if (e.key === 'Enter' || e.keyCode === 13) { window.sendChatMessage(); }
    };
    window.attachCommandAutocomplete(chatInput);
    chatInput.focus();
    console.log('[CHAT] input handler attached, focused');
  } else {
    console.warn('[CHAT] chatInput NOT FOUND');
  }
  // Load messages
  window._loadChatMessages(agentName);
  // Mark as read
  fetch('/api/chat/read', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + token },
    body: JSON.stringify({ from_agent: agentName })
  }).catch(function(e) { console.warn('[CHAT] mark read failed:', e); });
  // Start polling for new messages
  if (window._chatPollTimer) clearInterval(window._chatPollTimer);
  window._chatPollTimer = setInterval(function() {
    window._loadChatMessages(agentName, true);
  }, 3000);
};

window._loadChatMessages = function(agentName, pollOnly) {
  var token = localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || '';
  // Use pagination: initial load 30, subsequent polls use same endpoint
  var loadUrl = '/api/chat/messages?with=' + encodeURIComponent(agentName) + '&limit=30';
  // If we have oldest_id and it's not a poll, we could load more — but for DM, initial load is fine
  fetch(loadUrl, {
    headers: { 'Authorization': 'Bearer ' + token }
  }).then(function(r) { return r.json(); })
    .then(function(d) {
      var msgs = (d.messages || []).reverse(); // oldest first
      var container = document.getElementById('messages') || document.getElementById('chatMessages');
      if (!container) return;
      // Store pagination state for DM channel too
      var dmKey = '_dmPagination_' + agentName;
      window[dmKey] = { hasMore: d.has_more || false, oldestId: d.oldest_id || null, totalCount: d.total_count || 0 };
      if (!msgs.length) {
        if (!pollOnly) container.innerHTML = '<div style="color:var(--text3);font-size:13px;text-align:center;padding:20px;">Nincs üzenet. Írj valamit! 👋</div>';
        return;
      }
      // Dirty check: only re-render if message count or last ID changed
      var lastId = msgs.length ? msgs[msgs.length - 1].id : '';
      var cacheKey = '_dmLastId_' + agentName;
      if (pollOnly && window[cacheKey] === lastId && container.children.length > 0) {
        return; // No change — skip re-render (prevents flicker)
      }
      window[cacheKey] = lastId;
      
      var html = '';
      // Add "load more" indicator at top if there's more history
      if (d.has_more && d.oldest_id) {
        html += '<div id="dm-load-more" style="text-align:center;padding:8px;color:var(--text3);font-size:12px;cursor:pointer;">↑ Régebbi üzenetek betöltése…</div>';
      }
      var username = (authUser ? authUser.username : localStorage.getItem('a2a_username')) || 'zsolt';
      msgs.forEach(function(m) {
        var isSent = (m.sender === username);
        // Allow self-DM messages (sender === recipient === agentName)
        var senderName = esc(m.sender || '?');
        var content = esc(m.content || '');
        var time = m.created_at ? m.created_at.substring(11, 16) : '';
        var bg = isSent ? 'var(--primary)' : 'var(--surface)';
        var color = isSent ? '#fff' : 'var(--text)';
        var align = isSent ? 'margin-left:auto;' : 'margin-right:auto;';
        html += '<div style="max-width:75%;' + align + 'background:' + bg + ';color:' + color + ';border-radius:12px;padding:10px 14px;margin-bottom:6px;">';
        if (!isSent) html += '<div style="font-size:10px;font-weight:600;margin-bottom:2px;opacity:0.7;">' + senderName + '</div>';
        html += '<div style="font-size:13px;line-height:1.4;word-wrap:break-word;">' + content + '</div>';
        // ── File attachment card (DM view) ──
        if (m.attachment) {
          html += window._fileCardHtml(m.attachment, isSent);
        } else if (m.msg_type === 'file') {
          html += window._fileCardHtml({ file_name: m.content, mime_type: '', file_size: 0, url: '' }, isSent);
        }
        html += '<div style="font-size:9px;text-align:right;margin-top:2px;opacity:0.6;">' + time + '</div>';
        html += '</div>';
      });
      container.innerHTML = '<div style="width:100%;">' + html + '</div>';
      container.scrollTop = container.scrollHeight;
      // Attach click handler for "load more" in DM
      var dmLoadMore = document.getElementById('dm-load-more');
      if (dmLoadMore) {
        dmLoadMore.onclick = function() { _loadOlderDMMessages(agentName, container); };
      }
      // Also attach scroll listener for auto-load in DM
      if (d.has_more) {
        container.onscroll = function() {
          if (container.scrollTop < 50 && window[dmKey] && window[dmKey].hasMore && !_isLoadingOlder) {
            _loadOlderDMMessages(agentName, container);
          }
        };
      } else {
        container.onscroll = null;
      }
    }).catch(function(e) {});
};

function _loadOlderDMMessages(agentName, container) {
  if (_isLoadingOlder) return;
  var dmKey = '_dmPagination_' + agentName;
  var pg = window[dmKey];
  if (!pg || !pg.hasMore || !pg.oldestId) return;
  _isLoadingOlder = true;
  var token = localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || '';
  var loadMoreEl = document.getElementById('dm-load-more');
  if (loadMoreEl) loadMoreEl.textContent = 'Betöltés…';
  var prevScrollHeight = container.scrollHeight;
  var prevScrollTop = container.scrollTop;
  fetch('/api/chat/messages?with=' + encodeURIComponent(agentName) + '&limit=30&before_id=' + pg.oldestId, {
    headers: { 'Authorization': 'Bearer ' + token }
  }).then(function(r) { return r.json(); })
    .then(function(d) {
      if (!d || !d.messages) { _isLoadingOlder = false; return; }
      var olderMsgs = d.messages.reverse(); // chronological
      var username = (authUser ? authUser.username : localStorage.getItem('a2a_username')) || 'zsolt';
      // Build HTML for older messages
      var olderHtml = '';
      if (d.has_more && d.oldest_id) {
        olderHtml += '<div id="dm-load-more" style="text-align:center;padding:8px;color:var(--text3);font-size:12px;cursor:pointer;">↑ Régebbi üzenetek betöltése…</div>';
      }
      olderMsgs.forEach(function(m) {
        var isSent = (m.sender === username);
        var senderName = esc(m.sender || '?');
        var content = esc(m.content || '');
        var time = m.created_at ? m.created_at.substring(11, 16) : '';
        var bg = isSent ? 'var(--primary)' : 'var(--surface)';
        var color = isSent ? '#fff' : 'var(--text)';
        var align = isSent ? 'margin-left:auto;' : 'margin-right:auto;';
        olderHtml += '<div style="max-width:75%;' + align + 'background:' + bg + ';color:' + color + ';border-radius:12px;padding:10px 14px;margin-bottom:6px;">';
        if (!isSent) olderHtml += '<div style="font-size:10px;font-weight:600;margin-bottom:2px;opacity:0.7;">' + senderName + '</div>';
        olderHtml += '<div style="font-size:13px;line-height:1.4;word-wrap:break-word;">' + content + '</div>';
        olderHtml += '<div style="font-size:9px;text-align:right;margin-top:2px;opacity:0.6;">' + time + '</div>';
        olderHtml += '</div>';
      });
      // Prepend older messages to existing content
      var existingContent = container.innerHTML;
      container.innerHTML = '<div style="width:100%;">' + olderHtml + '</div>';
      // Append existing messages after older ones
      var wrapper = container.querySelector('div');
      if (wrapper) {
        wrapper.insertAdjacentHTML('beforeend', existingContent.replace(/^<div style="width:100%;">/, '').replace(/<\/div>$/, ''));
      }
      // Restore scroll position
      var newScrollHeight = container.scrollHeight;
      container.scrollTop = prevScrollTop + (newScrollHeight - prevScrollHeight);
      // Update pagination state
      window[dmKey] = { hasMore: d.has_more || false, oldestId: d.oldest_id || pg.oldestId, totalCount: pg.totalCount };
      // Re-attach load-more handler
      var newLoadMore = document.getElementById('dm-load-more');
      if (newLoadMore) {
        newLoadMore.onclick = function() { _loadOlderDMMessages(agentName, container); };
      }
      _isLoadingOlder = false;
    }).catch(function(e) {
      console.warn('[Chat DM] loadOlder error:', e);
      _isLoadingOlder = false;
      if (loadMoreEl) loadMoreEl.textContent = '↑ Régebbi üzenetek betöltése…';
    });
}

window.sendChatMessage = function() {
  console.log('[CHAT] sendChatMessage CALLED');
  var input = document.getElementById('chatInput');
  if (!input) { console.error('[CHAT] chatInput not found'); alert('Hiba: input nem található'); return; }
  if (!input.value.trim()) { console.log('[CHAT] empty input'); return; }
  var content = input.value.trim();
  var recipient = window._chatActiveContact;
  if (!recipient) { console.warn('[CHAT] no active contact'); alert('Válassz kontaktot!'); return; }
  var token = localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || '';
  console.log('[CHAT] sending to:', recipient, 'len:', content.length, 'token:', token ? 'yes' : 'NO');
  if (!token) { alert('Bejelentkezés szükséges!'); return; }
  var sendBtn = document.getElementById('chatSendBtn');
  if (sendBtn) sendBtn.disabled = true;
  fetch('/api/chat/send', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + token },
    body: JSON.stringify({ recipient: recipient, content: content })
  }).then(function(r) {
    console.log('[CHAT] response status:', r.status);
    if (sendBtn) sendBtn.disabled = false;
    if (r.status === 401) { alert('Lejárt! Jelentkezz be újra.'); location.reload(); return null; }
    return r.json();
  }).then(function(d) {
    if (!d) return;
    console.log('[CHAT] result:', d.ok, d.error || '');
    if (d.ok) {
      input.value = '';
      input.focus();
      window._loadChatMessages(recipient);
    } else {
      alert('Hiba: ' + (d.error || 'ismeretlen'));
    }
  }).catch(function(e) {
    console.error('[CHAT] error:', e);
    if (sendBtn) sendBtn.disabled = false;
    alert('Hiba: ' + e.message);
  });
};

window.memoryVectorSearch = function() {
  var token = localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || '';
  var q = (document.getElementById('memorySearchInput') || {}).value || '';
  if (!q.trim()) return;
  var resultsDiv = document.getElementById('memorySearchResults');
  if (resultsDiv) resultsDiv.innerHTML = '<div style="color:var(--text3);padding:12px;">Keresés... 🔍</div>';
  fetch('/api/memory-search?q=' + encodeURIComponent(q) + '&limit=10', {
    headers: { 'Authorization': 'Bearer ' + token }
  }).then(function(r) { return r.json(); })
    .then(function(d) {
      var html = '';
      var mesh = d.mesh_results || [];
      var agent = d.agent_results || [];
      if (mesh.length) {
        html += '<div style="margin-bottom:12px;"><div style="font-size:13px;font-weight:600;color:var(--primary);margin-bottom:6px;">📋 Delegation Memória (' + mesh.length + ')</div>';
        mesh.forEach(function(r) {
          var sim = r.similarity || '?';
          var sender = esc(r.source_agent || '?');
          var val = esc((r.memory_value || '').substring(0, 200));
          var ts = (r.created_at || '').substring(0, 19);
          html += '<div style="background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:10px;margin-bottom:6px;">';
          html += '<div style="display:flex;justify-content:space-between;margin-bottom:4px;">';
          html += '<span style="font-size:11px;color:var(--text3);">' + ts + ' • ' + sender + '</span>';
          html += '<span style="font-size:11px;color:var(--primary);font-weight:600;">sim=' + sim + '</span>';
          html += '</div>';
          html += '<div style="font-size:12px;color:var(--text);">' + val + '</div>';
          html += '</div>';
        });
        html += '</div>';
      }
      if (agent.length) {
        html += '<div><div style="font-size:13px;font-weight:600;color:var(--warning);margin-bottom:6px;">🧠 Tudásbázis (' + agent.length + ')</div>';
        agent.forEach(function(r) {
          var sim = r.similarity || '?';
          var cat = esc(r.category || '?');
          var title = esc((r.title || '').substring(0, 60));
          var content = esc((r.content || '').substring(0, 150));
          html += '<div style="background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:10px;margin-bottom:6px;">';
          html += '<div style="display:flex;justify-content:space-between;margin-bottom:4px;">';
          html += '<span style="font-size:11px;color:var(--text3);">' + cat + ' • ' + title + '</span>';
          html += '<span style="font-size:11px;color:var(--warning);font-weight:600;">sim=' + sim + '</span>';
          html += '</div>';
          html += '<div style="font-size:12px;color:var(--text);">' + content + '</div>';
          html += '</div>';
        });
        html += '</div>';
      }
      if (!mesh.length && !agent.length) {
        html = '<div style="color:var(--text3);padding:12px;">Nincs találat 😔</div>';
      }
      if (resultsDiv) resultsDiv.innerHTML = html;
    })
    .catch(function(e) {
      if (resultsDiv) resultsDiv.innerHTML = '<div style="color:var(--danger);padding:12px;">Hiba: ' + esc(e.message) + '</div>';
    });
};

window.syncSkills = function(evt) {
  var token = localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || '';
  var btn = (evt && evt.target) || (typeof event !== 'undefined' && event.target) || null;
  if (btn) { btn.disabled = true; btn.textContent = '🔄 Szinkron...'; }
  fetch('/api/skills/auto-sync', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + token },
    body: '{}'
  }).then(function(r) { return r.json(); })
    .then(function(d) {
      if (btn) { btn.disabled = false; btn.textContent = '🔄 Szinkron'; }
      if (d.ok || d.synced !== undefined) {
        loadMarveenPage('skills');
      } else {
        alert('Szinkron hiba: ' + (d.error || 'ismeretlen'));
      }
    })
    .catch(function(e) {
      if (btn) { btn.disabled = false; btn.textContent = '🔄 Szinkron'; }
      alert('Szinkron hiba: ' + e.message);
    });
};

window.showSkillDetail = function(skillId, skillName) {
  var body = document.getElementById('marveenModalBody');
  if (!body) return;
  body.innerHTML = '<div style="text-align:center;padding:40px;"><div style="font-size:32px;">⏳</div><p style="color:var(--text3);margin-top:8px;">Betöltés...</p></div>';
  var token = localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || '';
  // Fetch skill details
  fetch('/api/skills/search?q=' + encodeURIComponent(skillName), {
    headers: { 'Authorization': 'Bearer ' + token }
  }).then(function(r) { return r.json(); })
    .then(function(d) {
      var skills = (d.skills || d.results || []).filter(function(s) {
        return s.skill_name === skillName || s.skill_id === skillId;
      });
      if (!skills.length) {
        body.innerHTML = '<div style="padding:20px;color:var(--text3);">Nem található: ' + esc(skillName) + '</div>';
        return;
      }
      var s = skills[0];
      var sr = (s.success_rate || 1) * 100;
      var srColor = sr >= 90 ? '#4ade80' : sr >= 70 ? 'var(--warning)' : 'var(--danger)';
      var html = '<div style="padding:16px;">';
      // Header
      html += '<div style="margin-bottom:16px;">';
      html += '<h2 style="margin:0 0 4px;font-size:18px;">' + esc(s.display_name || s.skill_name || skillName) + '</h2>';
      html += '<div style="font-size:12px;color:var(--text3);">' + esc(s.skill_name || '') + '</div>';
      html += '</div>';
      // Description
      if (s.description) {
        html += '<div style="background:var(--surface2);border-radius:8px;padding:12px;margin-bottom:12px;font-size:13px;color:var(--text2);line-height:1.5;">' + esc(s.description) + '</div>';
      }
      // Metrics grid
      html += '<div style="display:grid;grid-template-columns:repeat(2,1fr);gap:8px;margin-bottom:16px;">';
      html += '<div style="background:var(--surface2);border-radius:8px;padding:10px;"><div style="font-size:10px;color:var(--text3);">Siker ráta</div><div style="font-size:16px;font-weight:700;color:' + srColor + ';">' + sr.toFixed(0) + '%</div></div>';
      html += '<div style="background:var(--surface2);border-radius:8px;padding:10px;"><div style="font-size:10px;color:var(--text3);">Késleltetés</div><div style="font-size:16px;font-weight:700;">' + esc(s.avg_latency_ms || 0) + 'ms</div></div>';
      html += '<div style="background:var(--surface2);border-radius:8px;padding:10px;"><div style="font-size:10px;color:var(--text3);">Költség</div><div style="font-size:16px;font-weight:700;">$' + esc((s.cost || 0).toFixed(4)) + '</div></div>';
      html += '<div style="background:var(--surface2);border-radius:8px;padding:10px;"><div style="font-size:10px;color:var(--text3);">Concurrent</div><div style="font-size:16px;font-weight:700;">' + esc(s.max_concurrent || 1) + '</div></div>';
      html += '</div>';
      // Agent
      html += '<div style="margin-bottom:12px;"><div style="font-size:10px;color:var(--text3);margin-bottom:4px;">Agent</div><span style="background:rgba(79,140,255,.15);color:var(--primary);padding:4px 10px;border-radius:6px;font-size:12px;">🤖 ' + esc(s.agent || s.node || '—') + '</span></div>';
      // Tags
      var tags = s.tags || [];
      if (tags.length) {
        html += '<div style="margin-bottom:16px;"><div style="font-size:10px;color:var(--text3);margin-bottom:4px;">Címkék</div><div style="display:flex;flex-wrap:wrap;gap:4px;">';
        tags.forEach(function(t) {
          html += '<span style="background:var(--surface);color:var(--text3);padding:2px 8px;border-radius:4px;font-size:11px;">#' + esc(t) + '</span>';
        });
        html += '</div></div>';
      }
      // Actions
      html += '<div style="display:flex;gap:8px;border-top:1px solid var(--border);padding-top:16px;">';
      html += '<button onclick="delegateToSkill(\'' + esc(s.skill_id || skillId) + '\',\'' + esc(skillName) + '\')" style="background:var(--primary);color:#fff;border:none;padding:10px 20px;border-radius:8px;cursor:pointer;font-size:13px;flex:1;">📤 Delegálás</button>';
      html += '<button onclick="rateSkill(\'' + esc(s.skill_id || skillId) + '\')" style="background:var(--surface2);color:var(--text);border:1px solid var(--border);padding:10px 20px;border-radius:8px;cursor:pointer;font-size:13px;">⭐ Értékelés</button>';
      html += '<button onclick="loadMarveenPage(\'skills\')" style="background:var(--surface2);color:var(--text);border:1px solid var(--border);padding:10px 20px;border-radius:8px;cursor:pointer;font-size:13px;">← Vissza</button>';
      html += '</div>';
      html += '</div>';
      body.innerHTML = html;
    })
    .catch(function(e) {
      body.innerHTML = '<div style="padding:20px;color:var(--danger);">Hiba: ' + esc(e.message) + '</div>';
    });
};

window.delegateToSkill = function(skillId, skillName) {
  var body = document.getElementById('marveenModalBody');
  if (!body) return;
  body.innerHTML = '<div style="padding:16px;">' +
    '<h2 style="margin:0 0 16px;font-size:18px;">📤 Delegálás: ' + esc(skillName) + '</h2>' +
    '<div style="margin-bottom:12px;"><label style="display:block;font-size:12px;color:var(--text3);margin-bottom:4px;">Feladat leírása *</label>' +
    '<textarea id="delegateTaskDesc" placeholder="Mit kell csinálni?" style="width:100%;padding:8px 10px;border-radius:6px;background:var(--surface2);border:1px solid var(--border);color:var(--text);font-size:13px;min-height:80px;resize:vertical;"></textarea></div>' +
    '<div style="margin-bottom:12px;"><label style="display:block;font-size:12px;color:var(--text3);margin-bottom:4px;">Prioritás</label>' +
    '<select id="delegatePriority" style="width:100%;padding:8px 10px;border-radius:6px;background:var(--surface2);border:1px solid var(--border);color:var(--text);font-size:13px;">' +
    '<option value="low">Alacsony</option><option value="normal" selected>Normál</option><option value="high">Magas</option></select></div>' +
    '<div style="display:flex;gap:8px;">' +
    '<button onclick="executeDelegate(\'' + esc(skillId) + '\')" style="background:var(--primary);color:#fff;border:none;padding:10px 24px;border-radius:8px;cursor:pointer;font-size:14px;flex:1;">Delegálás</button>' +
    '<button onclick="showSkillDetail(\'' + esc(skillId) + '\',\'' + esc(skillName) + '\')" style="background:var(--surface2);color:var(--text);border:1px solid var(--border);padding:10px 24px;border-radius:8px;cursor:pointer;font-size:14px;">Mégse</button>' +
    '</div></div>';
};

window.executeDelegate = function(skillId) {
  var desc = document.getElementById('delegateTaskDesc').value.trim();
  if (!desc) { alert('Feladat leírása kötelező!'); return; }
  var pri = document.getElementById('delegatePriority').value;
  var token = localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || '';
  fetch('/api/skills/' + skillId + '/delegate', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + token },
    body: JSON.stringify({ task_description: desc, priority: pri })
  }).then(function(r) { return r.json(); })
    .then(function(d) {
      if (d.ok || d.task_id) {
        alert('Delegálva! Task ID: ' + (d.task_id || 'OK'));
        loadMarveenPage('skills');
      } else {
        alert('Hiba: ' + (d.error || 'ismeretlen'));
      }
    })
    .catch(function(e) { alert('Hiba: ' + e.message); });
};

window.rateSkill = function(skillId) {
  var rating = prompt('Értékelés (1-5 csillag):', '5');
  if (!rating) return;
  var r = parseInt(rating, 10);
  if (isNaN(r) || r < 1 || r > 5) { alert('1-5 közötti számot adj meg!'); return; }
  var token = localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || '';
  fetch('/api/skills/' + skillId + '/rate', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + token },
    body: JSON.stringify({ rating: r })
  }).then(function(res) { return res.json(); })
    .then(function(d) {
      if (d.ok) { alert('Köszönöm az értékelést! ⭐' + r); }
      else { alert('Hiba: ' + (d.error || 'ismeretlen')); }
    })
    .catch(function(e) { alert('Hiba: ' + e.message); });
};

// ─── Ideas Board (Ötletláda) Functions ──────────────────────
window.showIdeaSubmitForm = function() {
  var body = document.getElementById('marveenModalBody');
  if (!body) return;
  var html = '<div style="padding:16px;">' +
    '<div style="margin-bottom:12px;"><label style="display:block;font-size:12px;color:var(--text3);margin-bottom:4px;">Cím *</label>' +
    '<input type="text" id="ideaTitle" placeholder="Mit javasolsz?" style="width:100%;padding:8px 10px;border-radius:6px;background:var(--surface2);border:1px solid var(--border);color:var(--text);font-size:14px;"></div>' +
    '<div style="margin-bottom:12px;"><label style="display:block;font-size:12px;color:var(--text3);margin-bottom:4px;">Leírás</label>' +
    '<textarea id="ideaDesc" placeholder="Részletes leírás..." style="width:100%;padding:8px 10px;border-radius:6px;background:var(--surface2);border:1px solid var(--border);color:var(--text);font-size:13px;min-height:80px;resize:vertical;"></textarea></div>' +
    '<div style="display:flex;gap:12px;margin-bottom:12px;">' +
    '<div style="flex:1;"><label style="display:block;font-size:12px;color:var(--text3);margin-bottom:4px;">Kategória</label>' +
    '<select id="ideaCategory" style="width:100%;padding:8px 10px;border-radius:6px;background:var(--surface2);border:1px solid var(--border);color:var(--text);font-size:13px;">' +
    '<option value="general">Általános</option><option value="feature">Új funkció</option><option value="bug">Hiba javítás</option><option value="optimization">Optimalizáció</option><option value="research">Kutatás</option><option value="infrastructure">Infrastruktúra</option></select></div>' +
    '<div style="flex:1;"><label style="display:block;font-size:12px;color:var(--text3);margin-bottom:4px;">Prioritás</label>' +
    '<select id="ideaPriority" style="width:100%;padding:8px 10px;border-radius:6px;background:var(--surface2);border:1px solid var(--border);color:var(--text);font-size:13px;">' +
    '<option value="low">Alacsony</option><option value="medium" selected>Közepes</option><option value="high">Magas</option></select></div></div>' +
    '<div style="margin-bottom:12px;"><label style="display:block;font-size:12px;color:var(--text3);margin-bottom:4px;">Címkék (vesszővel elválasztva)</label>' +
    '<input type="text" id="ideaTags" placeholder="pl. mesh, dashboard, ui" style="width:100%;padding:8px 10px;border-radius:6px;background:var(--surface2);border:1px solid var(--border);color:var(--text);font-size:13px;"></div>' +
    '<div style="display:flex;gap:8px;">' +
    '<button onclick="submitIdea()" style="background:var(--primary);color:#fff;border:none;padding:10px 24px;border-radius:8px;cursor:pointer;font-size:14px;flex:1;"> Beküldés</button>' +
    '<button onclick="loadMarveenPage(\'research\')" style="background:var(--surface2);color:var(--text);border:1px solid var(--border);padding:10px 24px;border-radius:8px;cursor:pointer;font-size:14px;">Mégse</button>' +
    '</div></div>';
  body.innerHTML = html;
};

window.submitIdea = function() {
  var title = document.getElementById('ideaTitle').value.trim();
  if (!title) { alert('Cím kötelező!'); return; }
  var desc = document.getElementById('ideaDesc').value.trim();
  var cat = document.getElementById('ideaCategory').value;
  var pri = document.getElementById('ideaPriority').value;
  var tagsStr = document.getElementById('ideaTags').value.trim();
  var tags = tagsStr ? tagsStr.split(',').map(function(t) { return t.trim(); }).filter(function(t) { return t; }) : [];
  var token = localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || '';
  fetch('/api/ideas', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + token },
    body: JSON.stringify({ title: title, description: desc, category: cat, priority: pri, tags: tags })
  }).then(function(r) { return r.json(); })
    .then(function(d) {
      if (d.ok) { loadMarveenPage('research'); }
      else { alert('Hiba: ' + (d.error || 'ismeretlen')); }
    })
    .catch(function(e) { alert('Hiba: ' + e.message); });
};

window.showIdeaDetail = function(id) {
  var idea = window._ideasCache && window._ideasCache[id];
  if (!idea) { loadMarveenPage('research'); return; }
  var token = localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || '';
  var priColor = idea.priority === 'high' ? 'var(--danger)' : idea.priority === 'low' ? 'var(--text3)' : 'var(--primary)';
  var stColors = { idea: '#60a5fa', approved: '#4ade80', in_progress: '#fbbf24', done: '#22c55e', rejected: '#ef4444' };
  var stNames = { idea: '💡 Ötlet', approved: '✅ Elfogadott', in_progress: '🔄 Folyamatban', done: '✔️ Kész', rejected: '❌ Elutasított' };
  var h = '';
  // Fejléc
  h += '<div style="font-size:15px;font-weight:700;margin-bottom:4px;color:var(--text);">' + esc(idea.title) + '</div>';
  h += '<div style="display:flex;gap:6px;flex-wrap:wrap;margin-bottom:12px;">' +
    '<span style="font-size:10px;padding:2px 8px;border-radius:8px;background:' + (stColors[idea.status] || '#888') + '22;color:' + (stColors[idea.status] || '#888') + ';">' + (stNames[idea.status] || idea.status) + '</span>' +
    '<span style="font-size:10px;padding:2px 8px;border-radius:8px;background:' + priColor + '22;color:' + priColor + ';">' + esc(idea.priority) + '</span>' +
    '<span style="font-size:10px;padding:2px 8px;border-radius:8px;background:var(--surface2);color:var(--text3);">' + esc(idea.category || 'general') + '</span>' +
    (idea.source_type === 'agent' ? '<span style="font-size:10px;padding:2px 8px;border-radius:8px;background:#3b1f5f;color:#c084fc;">🤖 agent</span>' : '') +
    '</div>';
  // Beépítettség jelzés
  if (idea.integrated) {
    h += '<div style="background:#14532d;border:1px solid #22c55e;border-radius:8px;padding:10px;margin-bottom:12px;">' +
      '<div style="font-size:12px;font-weight:700;color:#4ade80;">📦 BEÉPÜLT A REPÓBA</div>' +
      '<div style="font-size:11px;color:#86efac;margin-top:4px;">Fájl: <code style="color:#4ade80;">' + esc(idea.integrated_file || 'ideas/') + '</code></div>' +
      (idea.integrated_at ? '<div style="font-size:10px;color:#86efac;margin-top:2px;">📅 ' + esc(String(idea.integrated_at).substring(0, 19)) + '</div>' : '') +
      '</div>';
  } else if (idea.status === 'done') {
    h += '<div style="background:var(--surface2);border:1px solid var(--border);border-radius:8px;padding:10px;margin-bottom:12px;">' +
      '<div style="font-size:12px;font-weight:600;color:var(--text3);">⚠️ Kész, de NEM épült be a repóba</div>' +
      '<div style="font-size:10px;color:var(--text3);margin-top:2px;">A delegáció lefutott, de a kód nem került a repóba ( régi futás vagy nem-kód eredmény).</div>' +
      '</div>';
  }
  // Leírás
  if (idea.description) {
    h += '<div style="background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:10px;font-size:12px;color:var(--text2);line-height:1.5;margin-bottom:10px;white-space:pre-wrap;">' + esc(idea.description) + '</div>';
  }
  // Meta
  h += '<div style="display:flex;gap:10px;flex-wrap:wrap;font-size:10px;color:var(--text3);margin-bottom:12px;">' +
    '<span>👤 Beküldte: <strong style="color:var(--text2);">' + esc(idea.submitted_by) + '</strong></span>' +
    '<span>📅 ' + esc(String(idea.created_at || '').substring(0, 19)) + '</span>' +
    '<span>👍 ' + esc(idea.upvotes) + ' 👎 ' + esc(idea.downvotes) + ' (score: ' + (idea.score > 0 ? '+' : '') + esc(idea.score) + ')</span>' +
    '</div>';
  // Tags
  if (idea.tags && idea.tags.length) {
    h += '<div style="margin-bottom:12px;">' + idea.tags.map(function(t) { return '<span style="font-size:10px;background:var(--surface2);color:var(--text3);padding:2px 8px;border-radius:8px;margin-right:4px;">#' + esc(t) + '</span>'; }).join('') + '</div>';
  }
  // Szavazás gombok
  h += '<div style="display:flex;gap:8px;margin-bottom:12px;">' +
    '<button onclick="voteIdea(\'' + esc(id) + '\',\'up\');window._closeVaultModal()" style="flex:1;padding:8px;border-radius:8px;border:1px solid var(--border);background:var(--surface);color:#4ade80;cursor:pointer;font-size:13px;">👍 Fel</button>' +
    '<button onclick="voteIdea(\'' + esc(id) + '\',\'down\');window._closeVaultModal()" style="flex:1;padding:8px;border-radius:8px;border:1px solid var(--border);background:var(--surface);color:#ef4444;cursor:pointer;font-size:13px;">👎 Le</button>' +
    '</div>';
  // Akciók: status váltás mindig látható
  h += '<div style="font-size:11px;color:var(--text3);margin-bottom:4px;">Státusz módosítása:</div>';
  h += '<select id="idea-detail-status" style="width:100%;padding:8px;font-size:12px;border-radius:8px;background:var(--surface2);border:1px solid var(--border);color:var(--text);margin-bottom:12px;">' +
    '<option value="idea"' + (idea.status === 'idea' ? ' selected' : '') + '>💡 Ötlet</option>' +
    '<option value="approved"' + (idea.status === 'approved' ? ' selected' : '') + '>✅ Elfogadott</option>' +
    '<option value="in_progress"' + (idea.status === 'in_progress' ? ' selected' : '') + '>🔄 Folyamatban</option>' +
    '<option value="done"' + (idea.status === 'done' ? ' selected' : '') + '>✔️ Kész</option>' +
    '<option value="rejected"' + (idea.status === 'rejected' ? ' selected' : '') + '>❌ Elutasított</option>' +
    '</select>';
  // Elfogadás / megvalósítás gombok
  if (idea.status === 'idea') {
    h += '<button onclick="promoteIdeaToAgent(\'' + esc(id) + '\')" style="width:100%;padding:10px;border-radius:8px;border:none;background:var(--success);color:#fff;font-size:13px;font-weight:600;cursor:pointer;margin-bottom:8px;">🚀 Elfogadás → Kanban + mesh értesítés</button>';
  }
  if (idea.status === 'approved') {
    h += '<div style="background:#3b2f0f;border:1px solid var(--warning);border-radius:8px;padding:10px;margin-bottom:8px;">' +
      '<div style="font-size:11px;color:#fbbf24;font-weight:600;">✅ Elfogadva — beépítés-jóváhagyásra vár</div>' +
      '<div style="font-size:10px;color:var(--text3);margin-top:2px;">A megvalósítás csak a te jóváhagyásoddal indul.</div>' +
      '</div>';
    h += '<button onclick="implementIdea(\'' + esc(id) + '\')" style="width:100%;padding:14px;border-radius:8px;border:none;background:var(--primary);color:#fff;font-size:14px;font-weight:600;cursor:pointer;margin-bottom:8px;">🔨 BEÉPÍTÉS JÓVÁHAGYÁSA → delegáció a mesh-be</button>';
  }
  // Kommentek betöltés (ide, a modalba)
  h += '<div id="idea-detail-comments" style="margin-top:12px;"></div>';
  // Gombok
  window._showVaultModal('💡 Ötlet részletei', h, [
    { label: '🗑️ Törlés', onclick: 'deleteIdea(\'' + esc(id) + '\');window._closeVaultModal()' },
    { label: '💬 Komment írása', onclick: 'addIdeaComment(\'' + esc(id) + '\')' },
    { label: '💾 Státusz mentés', primary: true, onclick: 'window._saveIdeaStatus(\'' + esc(id) + '\')' }
  ]);
  // Kommentek betöltése
  fetch('/api/ideas/' + id + '/comments', { headers: { 'Authorization': 'Bearer ' + token } })
    .then(function(r) { return r.json(); })
    .then(function(d) {
      var cel = document.getElementById('idea-detail-comments');
      if (!cel) return;
      var ch = '<div style="font-size:11px;color:var(--text3);margin-bottom:6px;">💬 Kommentek:</div>';
      var comments = d.comments || [];
      if (!comments.length) { ch += '<div style="font-size:11px;color:var(--text3);">Még nincs komment.</div>'; }
      comments.forEach(function(c) {
        var isReview = c.author === 'coordinator_review';
        ch += '<div style="background:' + (isReview ? 'rgba(79,140,255,.08)' : 'var(--surface)') + ';border:1px solid ' + (isReview ? 'var(--primary)' : 'var(--border)') + ';border-radius:8px;padding:8px;margin-bottom:6px;font-size:11px;">' +
          '<div style="display:flex;justify-content:space-between;margin-bottom:4px;">' +
          '<strong style="color:' + (isReview ? 'var(--primary)' : 'var(--text2)') + ';">' + (isReview ? '🛡️ Koordinátor-review' : esc(c.author)) + '</strong>' +
          '<span style="color:var(--text3);font-size:9px;">' + esc(String(c.created_at || '').substring(0, 16)) + '</span>' +
          '</div>' +
          '<div style="color:var(--text2);white-space:pre-wrap;line-height:1.4;">' + esc(c.comment) + '</div>' +
          '</div>';
      });
      cel.innerHTML = ch;
    })
    .catch(function() {});
};

window._saveIdeaStatus = function(id) {
  var sel = document.getElementById('idea-detail-status');
  var newStatus = sel ? sel.value : 'idea';
  window._closeVaultModal();
  changeIdeaStatus(id, newStatus);
};

window.implementIdea = function(id) {
  if (!confirm('Megvalósítás: a koordinátor (nova) átnézi a Kanban kártyát és delegálja a feladatot a mesh-be. Indítsuk?')) return;
  var token = localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || '';
  window._closeVaultModal();
  fetch('/api/ideas/' + id + '/implement', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + token },
    body: JSON.stringify({})
  }).then(function(r) { return r.json(); })
    .then(function(d) {
      if (d.ok) {
        alert('🔨 Megvalósítás elindítva!\nDelegáció: ' + (d.task_id || '?') + '\nCél: ' + (d.assigned_to || 'bárki (available)'));
        loadMarveenPage('research');
      } else {
        alert('Hiba: ' + (d.error || 'ismeretlen'));
      }
    })
    .catch(function(e) { alert('Hiba: ' + e.message); });
};

window.voteIdea = function(id, vote) {
  var token = localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || '';
  fetch('/api/ideas/' + id + '/vote', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + token },
    body: JSON.stringify({ vote: vote })
  }).then(function(r) { return r.json(); })
    .then(function(d) {
      if (d.ok) {
        if (d.action === 'auto_implemented') {
          showToast('🗳️ Score +' + d.score + ' → ötlet automatikusan ELFOGADVA és megvalósításra indul!', 'success');
        } else if (d.action === 'auto_approved') {
          showToast('✅ Score +' + d.score + ' → elfogadva! ⏳ BEÉPÍTÉS-JÓVÁHAGYÁSRA VÁR — kattints a kártyára és jóváhagyás a beépítést!', 'success');
        } else if (d.action === 'auto_rejected') {
          showToast('❌ Score ' + d.score + ' → automatikusan elutasítva', 'error');
        } else {
          showToast('Szavazat rögzítve (score: ' + (d.score > 0 ? '+' : '') + d.score + ', küszöb: ±2, 48h után +1 is elég)', 'info');
        }
        loadMarveenPage('research');
      }
      else if (d.error === 'Already voted') { showToast('Már szavaztál erre az ötletre', 'info'); }
      else { console.error('Vote error:', d.error); }
    })
    .catch(function(e) { console.error('Vote failed:', e); });
};

window.changeIdeaStatus = function(id, status) {
  var token = localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || '';
  fetch('/api/ideas/' + id + '/status', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + token },
    body: JSON.stringify({ status: status })
  }).then(function(r) { return r.json(); })
    .then(function(d) {
      if (d.ok) { loadMarveenPage('research'); }
      else { console.error('Status change error:', d.error); }
    })
    .catch(function(e) { console.error('Status change failed:', e); });
};

window.deleteIdea = function(id) {
  if (!confirm('Biztosan törlöd ezt az ötletet?')) return;
  var token = localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || '';
  fetch('/api/ideas/' + id, {
    method: 'DELETE',
    headers: { 'Authorization': 'Bearer ' + token }
  }).then(function(r) { return r.json(); })
    .then(function(d) {
      if (d.ok) { loadMarveenPage('research'); }
      else { console.error('Delete error:', d.error); }
    })
    .catch(function(e) { console.error('Delete failed:', e); });
};

window.showIdeaComments = function(id) {
  var token = localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || '';
  fetch('/api/ideas/' + id + '/comments', { headers: { 'Authorization': 'Bearer ' + token } })
    .then(function(r) { return r.json(); })
    .then(function(d) {
      var body = document.getElementById('marveenModalBody');
      if (!body) return;
      var html = '<div style="padding:16px;">';
      html += '<button onclick="loadMarveenPage(\'research\')" style="background:var(--surface2);color:var(--text2);border:1px solid var(--border);padding:6px 14px;border-radius:8px;font-size:12px;cursor:pointer;margin-bottom:12px;">← Vissza</button>';
      html += '<h3 style="margin:0 0 12px;font-size:15px;">💬 Kommentek</h3>';
      var comments = d.comments || [];
      if (comments.length) {
        comments.forEach(function(c) {
          html += '<div style="background:var(--surface2);border:1px solid var(--border);border-radius:8px;padding:10px;margin-bottom:8px;">';
          html += '<div style="display:flex;align-items:center;gap:6px;margin-bottom:4px;">';
          html += '<strong style="font-size:12px;color:var(--text2);">' + esc(c.author) + '</strong>';
          html += '<span style="font-size:10px;color:var(--text3);margin-left:auto;">' + fmtTime(c.created_at) + '</span>';
          html += '</div>';
          html += '<div style="font-size:13px;color:var(--text);">' + esc(c.comment) + '</div>';
          html += '</div>';
        });
      } else {
        html += '<div style="text-align:center;padding:20px;color:var(--text3);">Még nincs komment</div>';
      }
      // Add comment form
      html += '<div style="margin-top:16px;">';
      html += '<textarea id="ideaCommentText" placeholder="Írj kommentet..." style="width:100%;padding:8px 10px;border-radius:6px;background:var(--surface2);border:1px solid var(--border);color:var(--text);font-size:13px;min-height:60px;resize:vertical;"></textarea>';
      html += '<button onclick="addIdeaComment(\'' + id + '\')" style="margin-top:8px;background:var(--primary);color:#fff;border:none;padding:8px 20px;border-radius:8px;cursor:pointer;font-size:13px;">Komment küldése</button>';
      html += '</div>';
      html += '</div>';
      body.innerHTML = html;
    })
    .catch(function(e) { alert('Hiba: ' + e.message); });
};

window.addIdeaComment = function(id) {
  var text = document.getElementById('ideaCommentText').value.trim();
  if (!text) { alert('Komment szöveg kötelező!'); return; }
  var token = localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || '';
  fetch('/api/ideas/' + id + '/comments', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + token },
    body: JSON.stringify({ comment: text })
  }).then(function(r) { return r.json(); })
    .then(function(d) {
      if (d.ok) { showIdeaComments(id); }
      else { alert('Hiba: ' + (d.error || 'ismeretlen')); }
    })
    .catch(function(e) { alert('Hiba: ' + e.message); });
};

window.promoteIdeaToAgent = function(id) {
  if (!confirm('Elfogadod ezt az ötletet és felveszed mesh agentként + Kanban kártyát készítesz?')) return;
  var token = localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || '';
  fetch('/api/ideas/' + id + '/promote-agent', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + token },
    body: JSON.stringify({})
  }).then(function(r) { return r.json(); })
    .then(function(d) {
      if (d.ok) {
        alert('Ötlet elfogadva! Kanban kártya létrehozva, mesh értesítve. Score: ' + d.score);
        loadMarveenPage('research');
      } else {
        alert('Hiba: ' + (d.error || 'ismeretlen'));
      }
    })
    .catch(function(e) { alert('Hiba: ' + e.message); });
};

window.importDiagnosticsAsIdeas = function() {
  if (!confirm('Diagnosztikai javaslatok importálása az ötletládába?')) return;
  var token = localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || '';
  fetch('/api/ideas/import-diagnostics', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + token },
    body: JSON.stringify({})
  }).then(function(r) { return r.json(); })
    .then(function(d) {
      if (d.ok) {
        alert(d.imported + ' diagnosztikai javaslat importálva!');
        loadMarveenPage('research');
      } else {
        alert('Hiba: ' + (d.error || 'ismeretlen'));
      }
    })
    .catch(function(e) { alert('Hiba: ' + e.message); });
};

window.generateDiagnosticReport = function() {
  var token = localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || '';
  fetch('/api/diagnostics/report', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + token },
    body: JSON.stringify({ report_type: 'on_demand' })
  }).then(function(r) { return r.json(); })
    .then(function(d) {
      if (d.error) { alert('Hiba: ' + d.error); }
      else { alert('Jelentés generálva: ' + (d.report_id || 'ok')); loadMarveenPage('diagnostics'); }
    })
    .catch(function(e) { alert('Hiba: ' + e.message); });
};

window.autoImplementSuggestions = function() {
  if (!confirm('Auto-implementálod az összes pending javaslatot?')) return;
  var token = localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || '';
  fetch('/api/diagnostics/auto-implement', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + token },
    body: JSON.stringify({})
  }).then(function(r) { return r.json(); })
    .then(function(d) {
      if (d.error) { alert('Hiba: ' + d.error); }
      else { alert('Implementálva: ' + (d.implemented || 0) + ' javaslat'); loadMarveenPage('diagnostics'); }
    })
    .catch(function(e) { alert('Hiba: ' + e.message); });
};

window.meshUpdateCheck = function() {
  loadMarveenPage('updates');
};

window.meshUpdatePull = function() {
  if (!confirm('Biztosan pull + deploy + restart? Ez leállítja a node-okat!')) return;
  var token = localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || '';
  fetch('/api/update-pull', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + token },
    body: JSON.stringify({})
  }).then(function(r) { return r.json(); })
    .then(function(d) {
      if (d.error) { alert('Hiba: ' + d.error); }
      else { alert('Pull: ' + (d.ok ? 'OK' : 'FAIL') + '\n' + (d.pull_output || '') + '\nDeploy: ' + JSON.stringify(d.deploy || {}) + '\nRestart: ' + d.restart); setTimeout(function() { loadMarveenPage('updates'); }, 3000); }
    })
    .catch(function(e) { alert('Hiba: ' + e.message); });
};

window.meshDeployAll = function() {
  if (!confirm('Deploy az összes peer node-ra?')) return;
  var token = localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || '';
  fetch('/api/deploy', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + token },
    body: JSON.stringify({})
  }).then(function(r) { return r.json(); })
    .then(function(d) {
      if (d.error) { alert('Hiba: ' + d.error); }
      else { alert('Deploy indítva: ' + (d.total || 0) + ' node'); }
    })
    .catch(function(e) { alert('Hiba: ' + e.message); });
};

window.sysinfoQueueAction = function(action) {
  var token = localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || '';
  if (!confirm(action === 'flush' ? 'Biztosan kiüríted a queue-t?' : 'Biztosan megtisztítod a queue-t?')) return;
  fetch('/api/queue/' + action, { method: 'POST', headers: { 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json' }, body: '{}' })
    .then(function(r) { return r.json(); })
    .then(function(d) { alert(action + ': ' + (d.ok ? 'OK' : JSON.stringify(d))); window._loadSysinfoExtras(); })
    .catch(function(e) { alert('Hiba: ' + e.message); });
};

window.showVaultAddModal = function() {
  var key = prompt('Vault entry kulcs:');
  if (!key) return;
  var value = prompt('Vault entry érték:');
  if (!value) return;
  var token = localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || '';
  fetch('/api/vault/store', { method: 'POST', headers: { 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json' }, body: JSON.stringify({ key: key, value: value }) })
    .then(function(r) { return r.json(); })
    .then(function(d) { if (d.error) { alert('Hiba: ' + d.error); } else { alert('Vault entry mentve'); loadMarveenPage('vault'); } })
    .catch(function(e) { alert('Hiba: ' + e.message); });
};

window.vaultDelete = function(id) {
  if (!confirm('Biztosan törlöd: ' + id + '?')) return;
  var token = localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || '';
  fetch('/api/vault/' + id, { method: 'DELETE', headers: { 'Authorization': 'Bearer ' + token } })
    .then(function(r) { return r.json(); })
    .then(function(d) { if (d.error) { alert('Hiba: ' + d.error); } else { alert('Törölve'); loadMarveenPage('vault'); } })
    .catch(function(e) { alert('Hiba: ' + e.message); });
};

// ── Vault Share: per-agent vault böngészés + mesh megosztás ──

window._vaultCurrentAgent = null; // null = helyi node

window._vaultToken = function() { return localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || ''; };
window._vaultCurrentAgent = null;
window._vaultEntriesCache = [];

window._vaultRenderEntries = function(entries, agentName, isLocal) {
  var el = document.getElementById('vault-entries');
  if (!el) return;
  window._vaultEntriesCache = entries || [];
  var h = '';
  // Kereső mező
  h += '<div style="display:flex;gap:8px;margin-bottom:10px;align-items:center;">' +
    '<input id="vault-search" type="text" placeholder="🔍 Keresés a bejegyzésekben…" oninput="vaultFilterEntries()" style="flex:1;background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:8px 12px;color:var(--text);font-size:12px;outline:none;">' +
    '<span id="vault-entry-count" style="font-size:10px;color:var(--text3);white-space:nowrap;"></span>' +
    '</div>';
  h += '<div id="vault-entry-list"></div>';
  el.innerHTML = h;
  window._vaultDrawList(agentName, isLocal);
};

window._vaultDrawList = function(agentName, isLocal) {
  var listEl = document.getElementById('vault-entry-list');
  if (!listEl) return;
  var q = (document.getElementById('vault-search') || {}).value || '';
  q = q.toLowerCase();
  var entries = window._vaultEntriesCache.filter(function(i) {
    var iid = i.id || i.key || i.label || i.name || '';
    return !q || String(iid).toLowerCase().indexOf(q) >= 0;
  });
  var countEl = document.getElementById('vault-entry-count');
  if (countEl) countEl.textContent = entries.length + ' / ' + window._vaultEntriesCache.length + ' tétel';
  if (!entries.length) {
    listEl.innerHTML = '<div style="color:var(--text3);font-size:12px;text-align:center;padding:24px;">' + (q ? 'Nincs találat: "' + esc(q) + '"' : 'Nincs bejegyzés ebben a vaultban<br><button onclick="vaultAddEntry(\'' + esc(window._vaultCurrentAgent || '') + '\')" style="margin-top:8px;background:var(--primary);color:#fff;border:none;padding:6px 14px;border-radius:6px;cursor:pointer;font-size:11px;">➕ Első tétel hozzáadása</button>') + '</div>';
    return;
  }
  var h = '';
  entries.forEach(function(i) {
    var iid = i.id || i.key || i.label || i.name || '';
    var type = i.type || i.mode || 'generic';
    var backend = i.backend || '';
    h += '<div class="vault-item" data-iid="' + esc(iid) + '" style="background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:10px 12px;margin-bottom:6px;">' +
      '<div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap;">' +
        '<strong style="font-size:12px;flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">🔑 ' + esc(iid) + '</strong>' +
        '<span style="font-size:9px;color:var(--text3);padding:2px 6px;background:var(--surface2);border-radius:8px;">' + esc(type) + (backend ? ' / ' + esc(backend) : '') + '</span>' +
        '<button onclick="vaultReveal(\'' + esc(iid) + '\', this)" title="Titok megjelenítése" style="font-size:10px;background:rgba(59,130,246,.15);color:var(--primary);border:1px solid var(--primary);padding:4px 8px;border-radius:4px;cursor:pointer;">👁️</button>' +
        '<button onclick="vaultCopy(\'' + esc(iid) + '\', this)" title="Vágólapra másolás" style="font-size:10px;background:var(--surface2);color:var(--text);border:1px solid var(--border);padding:4px 8px;border-radius:4px;cursor:pointer;">📋</button>' +
        '<button onclick="vaultShareEntry(\'' + esc(iid) + '\', \'' + esc(agentName) + '\')" title="Megosztás más node-okra" style="font-size:10px;background:rgba(139,92,246,.2);color:#c084fc;border:1px solid #c084fc;padding:4px 8px;border-radius:4px;cursor:pointer;">📤</button>' +
        '<button onclick="vaultDeleteEntry(\'' + esc(iid) + '\', \'' + esc(agentName) + '\')" title="Törlés ebből a vaultból" style="font-size:10px;background:rgba(239,68,68,.2);color:var(--danger);border:1px solid var(--danger);padding:4px 8px;border-radius:4px;cursor:pointer;">🗑️</button>' +
      '</div>' +
      '<div class="vault-value" style="display:none;margin-top:8px;padding:8px;background:var(--surface2);border-radius:6px;font-family:monospace;font-size:11px;word-break:break-all;color:var(--text);">⏳…</div>' +
    '</div>';
  });
  listEl.innerHTML = h;
};

window.vaultFilterEntries = function() {
  window._vaultDrawList(window._vaultCurrentAgent, window._vaultCurrentAgentIsLocal);
};

window.vaultReveal = function(name, btn) {
  var item = btn.closest('.vault-item');
  var valEl = item ? item.querySelector('.vault-value') : null;
  if (!valEl) return;
  if (valEl.style.display === 'none') {
    valEl.style.display = 'block';
    valEl.textContent = '⏳ betöltés…';
    var agent = window._vaultCurrentAgent || '';
    fetch('/api/vault/remote/' + encodeURIComponent(agent) + '/get', { method: 'POST', headers: { 'Authorization': 'Bearer ' + window._vaultToken(), 'Content-Type': 'application/json' }, body: JSON.stringify({ name: name }) })
      .then(function(r) { return r.json(); })
      .then(function(d) {
        if (d.ok && d.value != null) valEl.textContent = d.value;
        else valEl.textContent = '❌ ' + (d.error || 'nem elérhető');
      })
      .catch(function(e) { valEl.textContent = '❌ ' + e.message; });
    btn.textContent = '🙈';
  } else {
    valEl.style.display = 'none';
    btn.textContent = '👁️';
  }
};

window._copyToClipboard = function(text) {
  // 1. Modern async API (csak secure context: HTTPS/localhost — http://IP-n undefined!)
  if (navigator.clipboard && navigator.clipboard.writeText && window.isSecureContext !== false) {
    return navigator.clipboard.writeText(text).then(function() { return true; }).catch(function() { return window._copyFallback(text); });
  }
  return Promise.resolve(window._copyFallback(text));
};

window._copyFallback = function(text) {
  // 2. Legacy execCommand — insecure http://IP:port kontextusban is működik
  try {
    var ta = document.createElement('textarea');
    ta.value = text;
    ta.style.cssText = 'position:fixed;left:-9999px;top:0;opacity:0;';
    document.body.appendChild(ta);
    ta.focus(); ta.select();
    var ok = document.execCommand('copy');
    document.body.removeChild(ta);
    if (ok) return true;
  } catch (e) { /* folytatjuk a 3. opcióval */ }
  // 3. Végső eset: prompt ablak, ahol a user Ctrl+C-vel másol
  prompt('A böngésző nem engedélyezi az automatikus másolást.\nJelöld ki és másold ki (Ctrl+C / long-press → Copy):', text);
  return false;
};

window.vaultCopy = function(name, btn) {
  var agent = window._vaultCurrentAgent || '';
  fetch('/api/vault/remote/' + encodeURIComponent(agent) + '/get', { method: 'POST', headers: { 'Authorization': 'Bearer ' + window._vaultToken(), 'Content-Type': 'application/json' }, body: JSON.stringify({ name: name }) })
    .then(function(r) { return r.json(); })
    .then(function(d) {
      if (d.ok && d.value != null) {
        window._copyToClipboard(d.value).then(function(ok) {
          if (ok) { btn.textContent = '✅'; setTimeout(function() { btn.textContent = '📋'; }, 1200); }
          // prompt fallback esetén a prompt már megjelent — nincs extra alert
        });
      } else { alert('❌ ' + (d.error || 'nem elérhető')); }
    })
    .catch(function(e) { alert('Hiba: ' + e.message); });
};

window.vaultDeleteEntry = function(name, agentName) {
  if (!confirm('Biztosan törlöd "' + name + '" tételt a(z) ' + agentName + ' vaultból?')) return;
  var agent = window._vaultCurrentAgent || agentName;
  fetch('/api/vault/remote/' + encodeURIComponent(agent) + '/delete', { method: 'POST', headers: { 'Authorization': 'Bearer ' + window._vaultToken(), 'Content-Type': 'application/json' }, body: JSON.stringify({ name: name }) })
    .then(function(r) { return r.json(); })
    .then(function(d) {
      if (d.ok || d.deleted) { alert('✅ Törölve'); window.vaultSelectAgent(agent); }
      else alert('❌ ' + (d.error || 'nem sikerült'));
    })
    .catch(function(e) { alert('Hiba: ' + e.message); });
};

window.vaultAddEntry = function(agentName) {
  var name = prompt('Tétel neve (pl. MESH/PG_PASSWORD):');
  if (!name) return;
  var value = prompt('Titok értéke:');
  if (!value) return;
  var agent = window._vaultCurrentAgent || agentName || '';
  fetch('/api/vault/remote/' + encodeURIComponent(agent) + '/store', { method: 'POST', headers: { 'Authorization': 'Bearer ' + window._vaultToken(), 'Content-Type': 'application/json' }, body: JSON.stringify({ name: name, value: value }) })
    .then(function(r) { return r.json(); })
    .then(function(d) {
      if (d.ok) { alert('✅ Mentve a(z) ' + agent + ' vaultba'); window.vaultSelectAgent(agent); }
      else alert('❌ ' + (d.error || 'nem sikerült'));
    })
    .catch(function(e) { alert('Hiba: ' + e.message); });
};

window._vaultRenderAgentBar = function(meshNodes, current) {
  var el = document.getElementById('vault-agent-bar');
  if (!el) return;
  var h = '<span style="font-size:11px;color:var(--text3);margin-right:4px;">Agent vault:</span>';
  Object.keys(meshNodes).forEach(function(name) {
    var nd = meshNodes[name] || {};
    var active = (current === name) || (current === null && nd.local);
    var label = nd.local ? name + ' (helyi)' : name;
    var vs = nd.vault_status || {};
    var err = nd.error;
    var count = vs.entry_count != null ? vs.entry_count : (vs.entries || []).length;
    var dot = err ? '🔴' : (vs.initialized || nd.local ? '🟢' : '🟡');
    var title = err ? esc(name + ': ' + err) : esc(name + ': ' + count + ' tétel');
    h += '<button onclick="vaultSelectAgent(\'' + esc(name) + '\')" title="' + title + '" style="font-size:11px;padding:6px 12px;border-radius:14px;cursor:pointer;border:1px solid ' + (active ? 'var(--primary)' : 'var(--border)') + ';background:' + (active ? 'var(--primary)' : 'var(--surface)') + ';color:' + (active ? '#fff' : 'var(--text2)') + ';display:flex;align-items:center;gap:5px;">' + dot + ' ' + esc(label) + (err ? '' : ' <span style="opacity:.7;font-size:9px;">(' + esc(count) + ')</span>') + '</button>';
  });
  el.innerHTML = h;
};

window.vaultSelectAgent = function(nodeName) {
  window._vaultCurrentAgent = nodeName;
  var token = window._vaultToken();
  var bar = document.getElementById('vault-agent-bar');
  if (bar) bar.innerHTML = '<span style="font-size:11px;color:var(--text3);">⏳ Betöltés: ' + esc(nodeName) + '…</span>';
  var meshSection = document.getElementById('vault-mesh-section');
  if (meshSection) meshSection.innerHTML = '';
  fetch('/api/vault/remote/' + encodeURIComponent(nodeName), { headers: { 'Authorization': 'Bearer ' + token } })
    .then(function(r) { if (!r.ok) throw new Error('HTTP ' + r.status); return r.json(); })
    .then(function(d) {
      if (d.error) { alert('Hiba: ' + d.error); window.vaultShowMeshOverview(); return; }
      window._vaultCurrentAgentIsLocal = !!d.local;
      window._vaultRenderEntries(d.entries || [], d.node || nodeName, !!d.local);
      // Agent-bar frissítése a mesh áttekintésből
      return fetch('/api/vault/mesh', { headers: { 'Authorization': 'Bearer ' + token } })
        .then(function(r) { return r.json(); })
        .then(function(md) { window._vaultRenderAgentBar(md.nodes || {}, d.local ? null : d.node); })
        .catch(function() {});
    })
    .catch(function(e) {
      alert('Nem sikerült betölteni: ' + e.message);
      window.vaultShowMeshOverview();
    });
};

window.vaultShowMeshOverview = function(cb) {
  window._vaultCurrentAgent = null;
  var token = window._vaultToken();
  var meshSection = document.getElementById('vault-mesh-section');
  if (!meshSection) { if (cb) cb({}); return; }
  meshSection.innerHTML = '<div style="color:var(--text3);font-size:11px;">⏳ Mesh vault státusz betöltése…</div>';
  fetch('/api/vault/mesh', { headers: { 'Authorization': 'Bearer ' + token } })
    .then(function(r) { return r.json(); })
    .then(function(d) {
      var nodes = d.nodes || {};
      window._vaultRenderAgentBar(nodes, null);
      // Újrahasznosítjuk a vaultMesh renderer-t a szekcióhoz
      var h = '<h3 style="margin:12px 0 8px;font-size:13px;color:var(--text2);">🌐 Mesh áttekintés</h3>';
      Object.keys(nodes).forEach(function(name) {
        var nd = nodes[name] || {};
        var vs = nd.vault_status || {};
        var count = vs.entry_count != null ? vs.entry_count : (vs.entries || []).length;
        var healthy = vs.initialized || nd.local;
        h += '<div style="background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:10px 12px;margin-bottom:6px;display:flex;align-items:center;gap:10px;">' +
          '<div style="width:32px;height:32px;border-radius:50%;background:' + (healthy ? 'var(--success)' : 'var(--warning)') + ';display:flex;align-items:center;justify-content:center;">🔐</div>' +
          '<div style="flex:1;"><strong style="font-size:12px;">' + esc(name) + '</strong> <span style="font-size:10px;color:var(--text3);">' + (nd.local ? '(helyi)' : '(távoli)') + ' • ' + esc(count) + ' bejegyzés</span></div>' +
          '<button onclick="vaultSelectAgent(\'' + esc(name) + '\')" style="font-size:10px;background:var(--surface2);color:var(--text);border:1px solid var(--border);padding:4px 10px;border-radius:6px;cursor:pointer;">📂</button>' +
        '</div>';
      });
      meshSection.innerHTML = h;
      if (cb) cb(nodes);
    })
    .catch(function(e) { meshSection.innerHTML = '<div style="color:var(--danger);font-size:11px;">Hiba: ' + e.message + '</div>'; if (cb) cb({}); });
};

window.vaultShareEntry = function(name, fromNode) {
  var token = window._vaultToken();
  fetch('/api/vault/mesh', { headers: { 'Authorization': 'Bearer ' + token } })
    .then(function(r) { return r.json(); })
    .then(function(d) {
      var nodes = d.nodes || {};
      var options = Object.keys(nodes).filter(function(n) { return n !== fromNode; });
      if (!options.length) { alert('Nincs elérhető cél node'); return; }
      // Modal-alapú cél-választó
      var h = '<div style="font-size:12px;color:var(--text2);margin-bottom:10px;">Tétel: <strong style="color:var(--text);">' + esc(name) + '</strong><br>Forrás: ' + esc(fromNode || 'helyi') + '<br>Válaszd ki a cél node-okat:</div>';
      h += '<div style="display:flex;flex-direction:column;gap:6px;max-height:260px;overflow-y:auto;">';
      options.forEach(function(n) {
        var nd = nodes[n] || {};
        var vs = nd.vault_status || {};
        var count = vs.entry_count != null ? vs.entry_count : (vs.entries || []).length;
        h += '<label style="display:flex;align-items:center;gap:8px;padding:8px 10px;background:var(--surface);border:1px solid var(--border);border-radius:8px;cursor:pointer;font-size:12px;">' +
          '<input type="checkbox" class="vault-share-target" value="' + esc(n) + '" style="accent-color:var(--primary);">' +
          '<span>' + esc(n) + '</span>' +
          '<span style="font-size:10px;color:var(--text3);margin-left:auto;">' + esc(count) + ' tétel</span>' +
        '</label>';
      });
      h += '</div>';
      h += '<label style="display:flex;align-items:center;gap:8px;margin-top:10px;padding:8px 10px;background:var(--surface2);border-radius:8px;cursor:pointer;font-size:11px;color:var(--text3);">' +
        '<input type="checkbox" id="vault-share-all" style="accent-color:var(--primary);"> Mindet kijelöl (összes cél)' +
      '</label>';
      window._vaultShareModal = { name: name, from: fromNode, options: options };
      window._showVaultModal('📤 Megosztás', h, [
        { label: 'Mégse', onclick: 'window._closeVaultModal()' },
        { label: '📤 Megosztás', primary: true, onclick: 'window._vaultDoShare()' }
      ]);
    })
    .catch(function(e) { alert('Hiba: ' + e.message); });
};

window._vaultDoShare = function() {
  var m = window._vaultShareModal || {};
  var boxes = document.querySelectorAll('.vault-share-target:checked');
  var targets = [];
  for (var i = 0; i < boxes.length; i++) targets.push(boxes[i].value);
  if (!targets.length) { alert('Válassz legalább egy cél node-ot'); return; }
  var token = window._vaultToken();
  window._closeVaultModal();
  fetch('/api/vault/share', { method: 'POST', headers: { 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json' }, body: JSON.stringify({ name: m.name, targets: targets, from: m.from || undefined }) })
    .then(function(r) { return r.json(); })
    .then(function(res) {
      var results = res.results || {};
      var lines = Object.keys(results).map(function(t) {
        return t + ': ' + (results[t].ok ? '✅ megosztva' : '❌ ' + (results[t].error || 'hiba'));
      });
      alert('Megosztás eredménye:\n' + lines.join('\n'));
      window.vaultSelectAgent(m.from || window._vaultCurrentAgent);
    })
    .catch(function(e) { alert('Megosztási hiba: ' + e.message); });
};

// ── Vault modal rendszer ──
window._showVaultModal = function(title, bodyHtml, buttons) {
  var overlay = document.getElementById('vault-modal-overlay');
  if (!overlay) {
    overlay = document.createElement('div');
    overlay.id = 'vault-modal-overlay';
    overlay.style.cssText = 'position:fixed;inset:0;background:rgba(0,0,0,.55);z-index:9999;display:flex;align-items:center;justify-content:center;padding:16px;';
    overlay.innerHTML = '<div id="vault-modal" style="background:var(--bg,#16181d);border:1px solid var(--border,#2a2e38);border-radius:12px;padding:18px;max-width:420px;width:100%;max-height:85vh;overflow-y:auto;box-shadow:0 12px 40px rgba(0,0,0,.5);">' +
      '<div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:12px;">' +
        '<strong id="vault-modal-title" style="font-size:14px;color:var(--text,#e8eaf0);"></strong>' +
        '<button onclick="window._closeVaultModal()" style="background:none;border:none;color:var(--text3,#888);font-size:18px;cursor:pointer;">✕</button>' +
      '</div>' +
      '<div id="vault-modal-body" style="font-size:12px;"></div>' +
      '<div id="vault-modal-buttons" style="display:flex;gap:8px;justify-content:flex-end;margin-top:14px;"></div>' +
    '</div>';
    document.body.appendChild(overlay);
    overlay.addEventListener('click', function(ev) { if (ev.target === overlay) window._closeVaultModal(); });
  }
  overlay.style.display = 'flex';
  document.getElementById('vault-modal-title').textContent = title || '';
  document.getElementById('vault-modal-body').innerHTML = bodyHtml || '';
  var btnWrap = document.getElementById('vault-modal-buttons');
  btnWrap.innerHTML = '';
  (buttons || []).forEach(function(b) {
    var btn = document.createElement('button');
    btn.textContent = b.label;
    btn.style.cssText = 'padding:8px 16px;border-radius:8px;cursor:pointer;font-size:12px;border:1px solid ' + (b.primary ? 'var(--primary,#4f8cff)' : 'var(--border,#2a2e38)') + ';background:' + (b.primary ? 'var(--primary,#4f8cff)' : 'transparent') + ';color:' + (b.primary ? '#fff' : 'var(--text2,#aab)') + ';';
    btn.onclick = function() { try { eval(b.onclick); } catch (e) { console.error(e); } };
    btnWrap.appendChild(btn);
  });
  var allCb = document.getElementById('vault-share-all');
  if (allCb) allCb.onchange = function() {
    var boxes = document.querySelectorAll('.vault-share-target');
    for (var i = 0; i < boxes.length; i++) boxes[i].checked = allCb.checked;
  };
};

window._closeVaultModal = function() {
  var overlay = document.getElementById('vault-modal-overlay');
  if (overlay) overlay.style.display = 'none';
};

window.showRecoveryNoteModal = function() {
  var node = prompt('Target node:');
  if (!node) return;
  var note = prompt('Jegyzet szövege:');
  if (!note) return;
  var token = localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || '';
  fetch('/api/recovery-notes', { method: 'POST', headers: { 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json' }, body: JSON.stringify({ target_node: node, note: note }) })
    .then(function(r) { return r.json(); })
    .then(function(d) { if (d.error) { alert('Hiba: ' + d.error); } else { alert('Recovery note mentve'); window._loadSecurityExtras(); } })
    .catch(function(e) { alert('Hiba: ' + e.message); });
};

window.recoveryNoteRead = function(id) {
  var token = localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || '';
  fetch('/api/recovery-notes/' + id + '/read', { method: 'POST', headers: { 'Authorization': 'Bearer ' + token } })
    .then(function(r) { return r.json(); })
    .then(function(d) { window._loadSecurityExtras(); })
    .catch(function(e) { alert('Hiba: ' + e.message); });
};

window.onboardScan = function() {
  var token = localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || '';
  fetch('/api/onboard/scan', { method: 'POST', headers: { 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json' }, body: '{}' })
    .then(function(r) { return r.json(); })
    .then(function(d) { alert('Scan: ' + JSON.stringify(d).substring(0, 300)); loadMarveenPage('agents'); })
    .catch(function(e) { alert('Hiba: ' + e.message); });
};

window.wakeAgent = function(name) {
  var token = localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || '';
  fetch('/api/wake-agent', { method: 'POST', headers: { 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json' }, body: JSON.stringify({ agent: name }) })
    .then(function(r) { return r.json(); })
    .then(function(d) { alert('Wake ' + name + ': ' + (d.ok ? 'OK' : JSON.stringify(d))); })
    .catch(function(e) { alert('Hiba: ' + e.message); });
};

window.dailySummaryGenerate = function() {
  var token = localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || '';
  fetch('/api/daily-summary/generate', { method: 'POST', headers: { 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json' }, body: '{}' })
    .then(function(r) { return r.json(); })
    .then(function(d) { alert('Daily summary: ' + (d.ok ? 'generálva' : JSON.stringify(d))); })
    .catch(function(e) { alert('Hiba: ' + e.message); });
};

window.showProjectAddModal = function() {
  var title = prompt('Projekt címe:');
  if (!title) return;
  var url = prompt('Projekt URL (opcionális):') || '';
  var desc = prompt('Leírás (opcionális):') || '';
  var icon = prompt('Icon (emoji, opcionális):') || '📦';
  var token = localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || '';
  fetch('/api/projects', { method: 'POST', headers: { 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json' }, body: JSON.stringify({ title: title, url: url, description: desc, icon: icon }) })
    .then(function(r) { return r.json(); })
    .then(function(d) { if (d.error) { alert('Hiba: ' + d.error); } else { alert('Projekt létrehozva'); loadMarveenPage('projects'); } })
    .catch(function(e) { alert('Hiba: ' + e.message); });
};

window.projectDelete = function(pid) {
  if (!pid || !confirm('Biztosan törlöd: ' + pid + '?')) return;
  var token = localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || '';
  fetch('/api/projects/' + pid, { method: 'DELETE', headers: { 'Authorization': 'Bearer ' + token } })
    .then(function(r) { return r.json(); })
    .then(function(d) { if (d.error) { alert('Hiba: ' + d.error); } else { alert('Törölve'); loadMarveenPage('projects'); } })
    .catch(function(e) { alert('Hiba: ' + e.message); });
};

window.projectSync = function() {
  var token = localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || '';
  fetch('/api/projects/sync', { method: 'POST', headers: { 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json' }, body: '{}' })
    .then(function(r) { return r.json(); })
    .then(function(d) { alert('Sync: ' + JSON.stringify(d).substring(0, 200)); loadMarveenPage('projects'); })
    .catch(function(e) { alert('Hiba: ' + e.message); });
};

window.showLabelAddModal = function() {
  var name = prompt('Label neve:');
  if (!name) return;
  var color = prompt('Szín (hex, pl. #ef4444):') || '#3b82f6';
  var token = localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || '';
  fetch('/api/labels', { method: 'POST', headers: { 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json' }, body: JSON.stringify({ name: name, color: color }) })
    .then(function(r) { return r.json(); })
    .then(function(d) { if (d.error) { alert('Hiba: ' + d.error); } else { window._loadSysinfoExtras(); } })
    .catch(function(e) { alert('Hiba: ' + e.message); });
};

window.showDesiredStateModal = function() {
  var node = prompt('Node neve:');
  if (!node) return;
  var ssh = prompt('SSH target (opcionális):') || '';
  var token = localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || '';
  fetch('/api/desired-state/add', { method: 'POST', headers: { 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json' }, body: JSON.stringify({ node: node, ssh_target: ssh }) })
    .then(function(r) { return r.json(); })
    .then(function(d) { if (d.error) { alert('Hiba: ' + d.error); } else { window._loadNetworkExtras(); } })
    .catch(function(e) { alert('Hiba: ' + e.message); });
};

window.desiredStateRemove = function(node) {
  if (!confirm('Eltávolítod: ' + node + '?')) return;
  var token = localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || '';
  fetch('/api/desired-state/remove', { method: 'POST', headers: { 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json' }, body: JSON.stringify({ node: node }) })
    .then(function(r) { return r.json(); })
    .then(function(d) { window._loadNetworkExtras(); })
    .catch(function(e) { alert('Hiba: ' + e.message); });
};

window.onboardReject = function() {
  var node = prompt('Rejectelendő node neve:');
  if (!node) return;
  var token = localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || '';
  fetch('/api/onboard/reject', { method: 'POST', headers: { 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json' }, body: JSON.stringify({ node: node }) })
    .then(function(r) { return r.json(); })
    .then(function(d) { alert('Reject: ' + JSON.stringify(d).substring(0, 200)); loadMarveenPage('agents'); })
    .catch(function(e) { alert('Hiba: ' + e.message); });
};

window.showBudgetModal = function() {
  var budget = prompt('Havi budget ($):');
  if (!budget) return;
  var token = localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || '';
  fetch('/api/costops/budget', { method: 'POST', headers: { 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json' }, body: JSON.stringify({ monthly_budget: parseFloat(budget) }) })
    .then(function(r) { return r.json(); })
    .then(function(d) { if (d.error) { alert('Hiba: ' + d.error); } else { alert('Budget beállítva'); loadMarveenPage('costs'); } })
    .catch(function(e) { alert('Hiba: ' + e.message); });
};

window.codeReview = function(taskId) {
  var token = localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || '';
  fetch('/api/code-review', { method: 'POST', headers: { 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json' }, body: JSON.stringify({ task_id: taskId }) })
    .then(function(r) { return r.json(); })
    .then(function(d) { alert('Code review: ' + JSON.stringify(d).substring(0, 300)); })
    .catch(function(e) { alert('Hiba: ' + e.message); });
};

window.teamUpdate = function() {
  var token = localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || '';
  fetch('/api/team/update', { method: 'POST', headers: { 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json' }, body: '{}' })
    .then(function(r) { return r.json(); })
    .then(function(d) { alert('Team update: ' + JSON.stringify(d).substring(0, 200)); })
    .catch(function(e) { alert('Hiba: ' + e.message); });
};

window.showWorkflowCreateModal = function() {
  var name = prompt('Workflow neve:');
  if (!name) return;
  var consensus = prompt('Consensus (all/majority/any):') || 'all';
  var token = localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || '';
  fetch('/api/workflow', { method: 'POST', headers: { 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json' }, body: JSON.stringify({ name: name, consensus: consensus, tasks: [] }) })
    .then(function(r) { return r.json(); })
    .then(function(d) { if (d.error) { alert('Hiba: ' + d.error); } else { alert('Workflow létrehozva'); window._loadProjectsExtras(); } })
    .catch(function(e) { alert('Hiba: ' + e.message); });
};

window.workflowDelete = function(wid) {
  if (!wid || !confirm('Törlöd: ' + wid + '?')) return;
  var token = localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || '';
  fetch('/api/workflow/' + wid, { method: 'DELETE', headers: { 'Authorization': 'Bearer ' + token } })
    .then(function(r) { return r.json(); })
    .then(function(d) { window._loadProjectsExtras(); })
    .catch(function(e) { alert('Hiba: ' + e.message); });
};

window.authSync = function() {
  var token = localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || '';
  fetch('/api/auth/sync', { method: 'POST', headers: { 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json' }, body: '{}' })
    .then(function(r) { return r.json(); })
    .then(function(d) { alert('Auth sync: ' + JSON.stringify(d).substring(0, 200)); })
    .catch(function(e) { alert('Hiba: ' + e.message); });
};

window.voiceParse = function() {
  var text = prompt('Szöveg a voice parsernek:');
  if (!text) return;
  var token = localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || '';
  fetch('/api/voice/parse', { method: 'POST', headers: { 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json' }, body: JSON.stringify({ text: text }) })
    .then(function(r) { return r.json(); })
    .then(function(d) { alert('Voice parse: ' + JSON.stringify(d).substring(0, 300)); })
    .catch(function(e) { alert('Hiba: ' + e.message); });
};

window.promptSafetyCheck = function() {
  var text = prompt('Ellenőrizendő szöveg:');
  if (!text) return;
  var token = localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || '';
  fetch('/api/prompt-safety/check', { method: 'POST', headers: { 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json' }, body: JSON.stringify({ text: text }) })
    .then(function(r) { return r.json(); })
    .then(function(d) { alert('Safety: ' + JSON.stringify(d).substring(0, 300)); })
    .catch(function(e) { alert('Hiba: ' + e.message); });
};

window.webhookDeploy = function() {
  if (!confirm('Webhook deploy trigger?')) return;
  var token = localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || '';
  fetch('/api/webhook/deploy', { method: 'POST', headers: { 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json' }, body: '{}' })
    .then(function(r) { return r.json(); })
    .then(function(d) { alert('Webhook deploy: ' + JSON.stringify(d).substring(0, 200)); })
    .catch(function(e) { alert('Hiba: ' + e.message); });
};

window.showContextAddModal = function() {
  var key = prompt('Context kulcs:');
  if (!key) return;
  var value = prompt('Context érték:') || '';
  var token = localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || '';
  fetch('/api/context', { method: 'POST', headers: { 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json' }, body: JSON.stringify({ key: key, value: value }) })
    .then(function(r) { return r.json(); })
    .then(function(d) { if (d.error) { alert('Hiba: ' + d.error); } else { alert('Context mentve'); loadMarveenPage('insights-context-gate'); } })
    .catch(function(e) { alert('Hiba: ' + e.message); });
};

window.contextDelete = function(key) {
  if (!confirm('Törlöd: ' + key + '?')) return;
  var token = localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || '';
  fetch('/api/context/' + key, { method: 'DELETE', headers: { 'Authorization': 'Bearer ' + token } })
    .then(function(r) { return r.json(); })
    .then(function(d) { loadMarveenPage('insights-context-gate'); })
    .catch(function(e) { alert('Hiba: ' + e.message); });
};

window.showCronAddModal = function() {
  var name = prompt('Cron task neve:');
  if (!name) return;
  var schedule = prompt('Schedule (pl. 30m, every 2h, 0 9 * * *):') || '30m';
  var prompt_text = prompt('Prompt/parancs:') || '';
  var token = localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || '';
  fetch('/api/cron/add', { method: 'POST', headers: { 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json' }, body: JSON.stringify({ name: name, schedule: schedule, prompt: prompt_text }) })
    .then(function(r) { return r.json(); })
    .then(function(d) { if (d.error) { alert('Hiba: ' + d.error); } else { alert('Cron task hozzáadva'); loadMarveenPage('bgTasks'); } })
    .catch(function(e) { alert('Hiba: ' + e.message); });
};

window.showLlmBreakdownModal = function() {
  var task = prompt('Feladat leírása (mit bontsunk alfeladatokra?):');
  if (!task) return;
  var token = localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || '';
  fetch('/api/llm-breakdown', { method: 'POST', headers: { 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json' }, body: JSON.stringify({ task: task }) })
    .then(function(r) { return r.json(); })
    .then(function(d) {
      if (d.error) { alert('Hiba: ' + d.error); return; }
      var subtasks = d.subtasks || [];
      if (!subtasks.length) { alert('Nincs alfeladat'); return; }
      var msg = subtasks.length + ' alfeladat:\n\n';
      subtasks.forEach(function(s, i) {
        msg += (i + 1) + '. ' + (s.title || '?') + ' — ' + (s.description || '') + '\n';
        if (s.assignee) msg += '   → ' + s.assignee + '\n';
      });
      alert(msg);
    })
    .catch(function(e) { alert('Hiba: ' + e.message); });
};

window.taskCleanup = function() {
  if (!confirm('Törli a lejárt/completed feladatokat?')) return;
  var token = localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || '';
  fetch('/api/tasks/cleanup', { method: 'POST', headers: { 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json' }, body: '{}' })
    .then(function(r) { return r.json(); })
    .then(function(d) { alert('Cleanup: ' + JSON.stringify(d).substring(0, 200)); loadMarveenPage('tasks'); })
    .catch(function(e) { alert('Hiba: ' + e.message); });
};

window.trustAgent = function() {
  var agent = prompt('Agent neve:');
  if (!agent) return;
  var level = prompt('Trust level (trusted/untrusted/neutral):') || 'trusted';
  var token = localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || '';
  fetch('/api/trust', { method: 'POST', headers: { 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json' }, body: JSON.stringify({ agent: agent, level: level }) })
    .then(function(r) { return r.json(); })
    .then(function(d) { alert('Trust: ' + JSON.stringify(d).substring(0, 200)); })
    .catch(function(e) { alert('Hiba: ' + e.message); });
};

window.fleetImport = function() {
  if (!confirm('Flotta import indítása?')) return;
  var token = localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || '';
  fetch('/api/fleet/import', { method: 'POST', headers: { 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json' }, body: '{}' })
    .then(function(r) { return r.json(); })
    .then(function(d) { alert('Fleet import: ' + JSON.stringify(d).substring(0, 300)); })
    .catch(function(e) { alert('Hiba: ' + e.message); });
};

window.debugLog = function() {
  var level = prompt('Log level (INFO/WARN/ERROR):', 'INFO');
  if (!level) return;
  var msg = prompt('Log message:', '');
  if (!msg) return;
  var token = localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || '';
  fetch('/api/debug/log', { method: 'POST', headers: { 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json' }, body: JSON.stringify({ level: level, message: msg, category: 'dashboard' }) })
    .then(function(r) { return r.json(); })
    .then(function(d) { alert('Log: ' + JSON.stringify(d)); })
    .catch(function(e) { alert('Error: ' + e); });
};

window.modelFallbackError = function() {
  var token = localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || '';
  fetch('/api/model-fallback/error', { method: 'POST', headers: { 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json' }, body: JSON.stringify({ error: 'manual test error', model: 'test' }) })
    .then(function(r) { return r.json(); })
    .then(function(d) { alert('Model fallback: ' + JSON.stringify(d)); })
    .catch(function(e) { alert('Error: ' + e); });
};

window.sendAgentMessage = function() {
  var recipient = prompt('Címzett agent:', 'morzsa');
  if (!recipient) return;
  var text = prompt('Üzenet:', '');
  if (!text) return;
  var token = localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || '';
  fetch('/api/agent-message', { method: 'POST', headers: { 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json' }, body: JSON.stringify({ recipient: recipient, text: text, msg_type: 'a2a_message' }) })
    .then(function(r) { return r.json(); })
    .then(function(d) { alert('Message: ' + JSON.stringify(d)); })
    .catch(function(e) { alert('Error: ' + e); });
};

window.agentReply = function() {
  var msgId = prompt('Eredeti üzenet ID:', '');
  if (!msgId) return;
  var text = prompt('Válasz:', '');
  if (!text) return;
  var token = localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || '';
  fetch('/api/agent-reply', { method: 'POST', headers: { 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json' }, body: JSON.stringify({ message_id: msgId, text: text }) })
    .then(function(r) { return r.json(); })
    .then(function(d) { alert('Reply: ' + JSON.stringify(d)); })
    .catch(function(e) { alert('Error: ' + e); });
};

window.routeCalc = function() {
  var caps = prompt('Képességek (vesszővel elválasztva):', 'code,analysis');
  if (!caps) return;
  var token = localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || '';
  fetch('/api/route', { method: 'POST', headers: { 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json' }, body: JSON.stringify({ capabilities: caps.split(',').map(function(s) { return s.trim(); }) }) })
    .then(function(r) { return r.json(); })
    .then(function(d) { alert('Route: ' + JSON.stringify(d).substring(0, 500)); })
    .catch(function(e) { alert('Error: ' + e); });
};

// ─── Cost Chart ──
window.renderCostChart = function(canvasId, data) {
  var canvas = document.getElementById(canvasId);
  if (!canvas) return;
  var ctx = canvas.getContext('2d');
  var w = canvas.width = canvas.offsetWidth || 400;
  var h = canvas.height = 200;
  ctx.clearRect(0, 0, w, h);
  
  var summary = data.summary || {};
  var byAgent = summary.by_agent || {};
  var agents = Object.keys(byAgent);
  var costs = agents.map(function(a) { return byAgent[a] || 0; });
  var maxCost = Math.max.apply(null, costs.concat([0.01]));
  
  // Background
  ctx.fillStyle = 'rgba(255,255,255,0.03)';
  ctx.fillRect(0, 0, w, h);
  
  // Grid lines
  ctx.strokeStyle = 'rgba(255,255,255,0.05)';
  ctx.lineWidth = 1;
  for (var g = 0; g < 4; g++) {
    var gy = h - (g + 1) * h / 4;
    ctx.beginPath();
    ctx.moveTo(40, gy);
    ctx.lineTo(w - 10, gy);
    ctx.stroke();
  }
  
  // Bars
  var colors = ['#3b82f6', '#10b981', '#f59e0b', '#ef4444', '#8b5cf6'];
  var barW = agents.length > 0 ? Math.min(60, (w - 60) / agents.length) : 0;
  for (var i = 0; i < agents.length; i++) {
    var barH = maxCost > 0 ? (costs[i] / maxCost) * (h - 40) : 0;
    var x = 50 + i * (barW + 10);
    var y = h - 20 - barH;
    ctx.fillStyle = colors[i % colors.length];
    ctx.fillRect(x, y, barW, barH);
    // Label
    ctx.fillStyle = 'rgba(255,255,255,0.6)';
    ctx.font = '11px sans-serif';
    ctx.textAlign = 'center';
    ctx.fillText(agents[i].substring(0, 6), x + barW/2, h - 5);
    ctx.fillText('$' + costs[i].toFixed(4), x + barW/2, y - 5);
  }
  
  // Title
  ctx.fillStyle = 'rgba(255,255,255,0.8)';
  ctx.font = 'bold 12px sans-serif';
  ctx.textAlign = 'left';
  ctx.fillText('Költség agentenként ($' + (summary.total_cost_usd || 0).toFixed(4) + ')', 10, 15);
};

window.loadCostChart = function() {
  var token = localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || '';
  fetch('/api/insights/cost', { headers: { 'Authorization': 'Bearer ' + token } })
    .then(function(r) { return r.json(); })
    .then(function(d) {
      window.renderCostChart('costChartCanvas', d);
    })
    .catch(function(e) { console.error('Cost chart error:', e); });
};


// --- Activity filter ---
window.filterActivityTable = function() {
  var q = (document.getElementById('activityFilter').value || '').toLowerCase();
  var wrap = document.getElementById('activityTableWrap');
  if (!wrap) return;
  var rows = wrap.querySelectorAll('tr');
  for (var i = 1; i < rows.length; i++) {
    var text = rows[i].innerText.toLowerCase();
    rows[i].style.display = text.indexOf(q) >= 0 ? '' : 'none';
  }
};

// --- Keyboard Shortcuts ---
window.handleKeyboard = function(e) {
  if (e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA') return;
  if (e.ctrlKey || e.metaKey || e.altKey) return;
  var key = e.key.toLowerCase();
  switch(key) {
    case 'k': e.preventDefault(); document.getElementById('globalSearchInput').focus(); break;
    case 'g': loadMarveenPage('overview'); break;
    case 't': toggleTheme(); break;
    case 'r': loadMarveenPage('kanban'); break;
    case 'a': loadMarveenPage('alerts'); break;
    case 'd': loadMarveenPage('delegations'); break;
    case 'm': loadMarveenPage('messages'); break;
    case 'n': loadMarveenPage('nodes'); break;
    case 'c': loadMarveenPage('insights-cost'); break;
    case '?': showToast('Shortcuts: K=Keresés G=Áttekintés T=Téma R=Kanban A=Alerts D=Delegáció M=Üzenet N=Node-ok C=Költség', 'info'); break;
  }
};
document.addEventListener('keydown', window.handleKeyboard);

// --- Auto-refresh active modal ---
window.autoRefreshInterval = null;
window.startAutoRefresh = function() {
  if (window.autoRefreshInterval) clearInterval(window.autoRefreshInterval);
  window.autoRefreshInterval = setInterval(function() {
    if (currentMarveenPage && document.getElementById('marveenModal').style.display !== 'none') {
      var page = currentMarveenPage;
      var apiMapEntry = apiMap[page];
      if (apiMapEntry && apiMapEntry !== 'none') {
        var token = localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || '';
        fetch(apiMapEntry, { headers: { 'Authorization': 'Bearer ' + token } })
          .then(function(r) { return r.json(); })
          .then(function(d) {
            var renderer = renderers[page];
            if (renderer) {
              document.getElementById('marveenModalBody').innerHTML = renderer(d);
            }
          })
          .catch(function() {});
      }
    }
  }, 30000);
};
window.startAutoRefresh();

// --- Notification badge ---
window.updateNotificationBadge = function() {
  var token = localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || '';
  fetch('/api/overview', { headers: { 'Authorization': 'Bearer ' + token } })
    .then(function(r) { return r.json(); })
    .then(function(d) {
      var pending = (d.tasks_pending || 0) + (d.pending_approvals || 0);
      var badge = document.getElementById('notifBadge');
      if (!badge) {
        badge = document.createElement('span');
        badge.id = 'notifBadge';
        badge.style.cssText = 'position:fixed;top:12px;right:12px;background:var(--danger);color:#fff;border-radius:12px;padding:2px 8px;font-size:10px;font-weight:700;z-index:99999;cursor:pointer;';
        badge.onclick = function() { loadMarveenPage('delegations'); };
        document.body.appendChild(badge);
      }
      if (pending > 0) {
        badge.textContent = pending;
        badge.style.display = 'block';
      } else {
        badge.style.display = 'none';
      }
    })
    .catch(function() {});
  setTimeout(window.updateNotificationBadge, 60000);
};
window.updateNotificationBadge();

// ─── Token Usage Chart ──
window.renderTokenChart = function(canvasId, data) {
  var canvas = document.getElementById(canvasId);
  if (!canvas) return;
  var ctx = canvas.getContext('2d');
  var w = canvas.width = canvas.offsetWidth || 400;
  var h = canvas.height = 200;
  ctx.clearRect(0, 0, w, h);
  
  var byAgent = data.by_agent || {};
  var agents = Object.keys(byAgent);
  if (!agents.length) {
    ctx.fillStyle = 'rgba(255,255,255,0.4)';
    ctx.font = '13px sans-serif';
    ctx.textAlign = 'center';
    ctx.fillText('Nincs token adat', w/2, h/2);
    return;
  }
  
  var totals = agents.map(function(a) {
    var d = byAgent[a] || {};
    return (d.input || 0) + (d.output || 0);
  });
  var maxT = Math.max.apply(null, totals.concat([1]));
  
  // Background
  ctx.fillStyle = 'rgba(255,255,255,0.03)';
  ctx.fillRect(0, 0, w, h);
  
  // Stacked bars (input + output)
  var colors = ['#3b82f6', '#10b981', '#f59e0b', '#ef4444', '#8b5cf6'];
  var barW = agents.length > 0 ? Math.min(60, (w - 60) / agents.length) : 0;
  for (var i = 0; i < agents.length; i++) {
    var d = byAgent[agents[i]] || {};
    var inT = d.input || 0;
    var outT = d.output || 0;
    var inH = maxT > 0 ? (inT / maxT) * (h - 40) : 0;
    var outH = maxT > 0 ? (outT / maxT) * (h - 40) : 0;
    var x = 50 + i * (barW + 10);
    var yOut = h - 20 - outH;
    var yIn = yOut - inH;
    
    // Output (bottom)
    ctx.fillStyle = colors[i % colors.length];
    ctx.fillRect(x, yOut, barW, outH);
    // Input (top)
    ctx.fillStyle = colors[(i + 2) % colors.length];
    ctx.fillRect(x, yIn, barW, inH);
    
    // Labels
    ctx.fillStyle = 'rgba(255,255,255,0.6)';
    ctx.font = '11px sans-serif';
    ctx.textAlign = 'center';
    ctx.fillText(agents[i].substring(0, 6), x + barW/2, h - 5);
    ctx.fillText(String(totals[i]), x + barW/2, yIn - 5);
  }
  
  // Title
  ctx.fillStyle = 'rgba(255,255,255,0.8)';
  ctx.font = 'bold 12px sans-serif';
  ctx.textAlign = 'left';
  ctx.fillText('Token használat (' + (data.total_tokens || 0) + ' total)', 10, 15);
};

window.loadTokenChart = function() {
  var token = localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || '';
  fetch('/api/token-usage', { headers: { 'Authorization': 'Bearer ' + token } })
    .then(function(r) { return r.json(); })
    .then(function(d) {
      window.renderTokenChart('tokenChartCanvas', d);
    })
    .catch(function(e) { console.error('Token chart error:', e); });
};

// ─── Alert Rule Editor ──
window.showAlertRuleEditor = function(ruleId) {
  var token = localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || '';
  fetch('/api/alerts', { headers: { 'Authorization': 'Bearer ' + token } })
    .then(function(r) { return r.json(); })
    .then(function(d) {
      var rules = d.rules || [];
      var rule = ruleId ? rules.filter(function(r) { return r.id === ruleId; })[0] : null;
      var html = '<div style="padding:16px;">';
      html += '<h3 style="margin:0 0 12px;">' + (rule ? 'Szabály szerkesztése' : 'Új riasztási szabály') + '</h3>';
      html += '<label style="display:block;margin-bottom:8px;">Név:<br><input id="arName" type="text" value="' + (rule ? (rule.name || '') : '') + '" style="width:100%;padding:6px;background:var(--surface2);border:1px solid var(--border);border-radius:4px;color:var(--text);"></label>';
      html += '<label style="display:block;margin-bottom:8px;">Leírás:<br><input id="arDesc" type="text" value="' + (rule ? (rule.description || '') : '') + '" style="width:100%;padding:6px;background:var(--surface2);border:1px solid var(--border);border-radius:4px;color:var(--text);"></label>';
      html += '<label style="display:block;margin-bottom:8px;">Kategória:<br><select id="arCat" style="width:100%;padding:6px;background:var(--surface2);border:1px solid var(--border);border-radius:4px;color:var(--text);"><option value="connectivity">Kapcsolat</option><option value="performance">Teljesítmény</option><option value="security">Biztonság</option><option value="cost">Költség</option></select></label>';
      html += '<label style="display:block;margin-bottom:8px;">Prioritás:<br><select id="arPri" style="width:100%;padding:6px;background:var(--surface2);border:1px solid var(--border);border-radius:4px;color:var(--text);"><option value="low">Alacsony</option><option value="medium" selected>Közepes</option><option value="high">Magas</option><option value="critical">Kritikus</option></select></label>';
      html += '<div style="display:flex;gap:8px;margin-top:12px;">';
      html += '<button onclick="saveAlertRule(\'' + (ruleId || '') + '\')" style="background:var(--primary);color:#fff;border:none;padding:8px 16px;border-radius:6px;cursor:pointer;">Mentés</button>';
      html += '<button onclick="document.getElementById(\'marveenModal\').style.display=\'none\'" style="background:var(--surface2);color:var(--text);border:1px solid var(--border);padding:8px 16px;border-radius:6px;cursor:pointer;">Mégse</button>';
      html += '</div></div>';
      var modal = document.getElementById('marveenModal');
      if (modal) {
        modal.querySelector('.file-modal').innerHTML = html;
        modal.style.display = 'flex';
      }
    });
};

window.saveAlertRule = function(ruleId) {
  var name = document.getElementById('arName').value;
  var desc = document.getElementById('arDesc').value;
  var cat = document.getElementById('arCat').value;
  var pri = document.getElementById('arPri').value;
  var token = localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || '';
  fetch('/api/alerts/rules/' + (ruleId || 'new'), {
    method: 'POST',
    headers: { 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json' },
    body: JSON.stringify({ name: name, description: desc, category: cat, priority: pri, enabled: true })
  })
    .then(function(r) { return r.json(); })
    .then(function(d) {
      alert('Szabály mentve: ' + JSON.stringify(d));
      loadMarveenPage('alerts');
      document.getElementById('marveenModal').style.display = 'none';
    })
    .catch(function(e) { alert('Hiba: ' + e); });
};

// ─── Toast Notifications ──
window.showToast = function(msg, type) {
  type = type || 'info';
  var toast = document.createElement('div');
  var colors = { info: 'var(--primary)', success: 'var(--success)', warning: 'var(--warning)', error: 'var(--danger)' };
  var icons = { info: 'ℹ️', success: '✅', warning: '⚠️', error: '❌' };
  toast.style.cssText = 'position:fixed;bottom:20px;right:20px;background:var(--surface);border:1px solid ' + (colors[type] || colors.info) + ';border-radius:8px;padding:12px 16px;z-index:99999;box-shadow:0 4px 12px rgba(0,0,0,0.3);font-size:13px;max-width:350px;display:flex;align-items:center;gap:8px;transition:opacity 0.3s,transform 0.3s;opacity:0;transform:translateY(20px);';
  toast.innerHTML = '<span>' + (icons[type] || icons.info) + '</span><span style="flex:1;color:var(--text);">' + esc(msg) + '</span>';
  document.body.appendChild(toast);
  setTimeout(function() { toast.style.opacity = '1'; toast.style.transform = 'translateY(0)'; }, 10);
  setTimeout(function() {
    toast.style.opacity = '0';
    toast.style.transform = 'translateY(20px)';
    setTimeout(function() { if (toast.parentNode) toast.parentNode.removeChild(toast); }, 300);
  }, 4000);
};

// ─── Loading Spinner ──
window.showLoading = function() {
  var existing = document.getElementById('loadingOverlay');
  if (existing) return;
  var overlay = document.createElement('div');
  overlay.id = 'loadingOverlay';
  overlay.style.cssText = 'position:fixed;top:0;left:0;width:100%;height:100%;background:rgba(0,0,0,0.3);z-index:99998;display:flex;align-items:center;justify-content:center;';
  overlay.innerHTML = '<div style="width:40px;height:40px;border:3px solid var(--surface2);border-top:3px solid var(--primary);border-radius:50%;animation:spin 1s linear infinite;"></div><style>@keyframes spin{0%{transform:rotate(0)}100%{transform:rotate(360deg)}}</style>';
  document.body.appendChild(overlay);
};

window.hideLoading = function() {
  var overlay = document.getElementById('loadingOverlay');
  if (overlay) overlay.parentNode.removeChild(overlay);
};

// ─── Export CSV/JSON ──
window.exportData = function(data, filename, format) {
  format = format || 'json';
  var content, mime;
  if (format === 'csv') {
    var keys = Object.keys(data[0] || {});
    var rows = [keys.join(',')];
    data.forEach(function(item) {
      rows.push(keys.map(function(k) { return '"' + String(item[k] || '').replace(/"/g, '\"') + '"'; }).join(','));
    });
    content = rows.join('\n');
    mime = 'text/csv';
  } else {
    content = JSON.stringify(data, null, 2);
    mime = 'application/json';
  }
  var blob = new Blob([content], { type: mime });
  var url = URL.createObjectURL(blob);
  var a = document.createElement('a');
  a.href = url;
  a.download = filename || ('export_' + Date.now() + '.' + format);
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
  showToast('Exportálva: ' + (filename || 'export'), 'success');
};

window.exportCurrentPage = function() {
  var page = currentPage || '';
  var token = localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || '';
  var apiUrl = apiMap[page];
  if (!apiUrl) { showToast('Nem exportálható oldal', 'warning'); return; }
  fetch(apiUrl, { headers: { 'Authorization': 'Bearer ' + token } })
    .then(function(r) { return r.json(); })
    .then(function(d) {
      var data = d.nodes || d.agents || d.tasks || d.entries || d.items || d.rules || d.boards || d.ideas || [d];
      if (!Array.isArray(data)) data = [data];
      exportData(data, page + '_export_' + Date.now() + '.json', 'json');
    })
    .catch(function(e) { showToast('Export hiba: ' + e, 'error'); });
};

// ─── Global Search ──
window.globalSearch = function(query) {
  if (!query || query.length < 2) return;
  var token = localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || '';
  var results = [];
  var pages = ['nodes', 'agents', 'delegations', 'tasks', 'kanban', 'ideas', 'labels', 'vault'];
  var pending = pages.length;
  pages.forEach(function(page) {
    var apiUrl = apiMap[page];
    if (!apiUrl) { pending--; return; }
    fetch(apiUrl, { headers: { 'Authorization': 'Bearer ' + token } })
      .then(function(r) { return r.json(); })
      .then(function(d) {
        var items = d.nodes || d.agents || d.tasks || d.boards || d.ideas || d.entries || d.items || d.labels || [];
        if (Array.isArray(items)) {
          items.forEach(function(item) {
            var str = JSON.stringify(item).toLowerCase();
            if (str.indexOf(query.toLowerCase()) >= 0) {
              results.push({ page: page, item: item });
            }
          });
        }
        pending--;
        if (pending === 0) {
          showSearchResults(results, query);
        }
      })
      .catch(function() { pending--; if (pending === 0) showSearchResults(results, query); });
  });
};

window.showSearchResults = function(results, query) {
  var html = '<div style="padding:16px;">';
  html += '<h3 style="margin:0 0 12px;">🔍 Keresés: "' + esc(query) + '" — ' + results.length + ' találat</h3>';
  if (!results.length) {
    html += '<p style="color:var(--text3);">Nincs találat</p>';
  } else {
    results.forEach(function(r) {
      var name = r.item.node_name || r.item.name || r.item.title || r.item.subject || r.item.key || r.item.id || '?';
      html += '<div style="background:var(--surface2);padding:8px 12px;border-radius:6px;margin-bottom:4px;cursor:pointer;" onclick="loadMarveenPage(\'' + r.page + '\'); document.getElementById(\'marveenModal\').style.display=\'none\';">';
      html += '<span style="font-size:11px;color:var(--text3);">[' + r.page + ']</span> ';
      html += '<span style="font-size:13px;">' + esc(String(name)) + '</span>';
      html += '</div>';
    });
  }
  html += '</div>';
  var modal = document.getElementById('marveenModal');
  if (modal) {
    modal.querySelector('.file-modal').innerHTML = html;
    modal.style.display = 'flex';
  }
};

// ─── Dark/Light Theme Toggle ──
window.toggleTheme = function() {
  var root = document.documentElement;
  var current = root.getAttribute('data-theme') || 'dark';
  var next = current === 'dark' ? 'light' : 'dark';
  root.setAttribute('data-theme', next);
  localStorage.setItem('a2a_theme', next);
  applyTheme(next);
  showToast('Téma: ' + (next === 'dark' ? '🌙 Sötét' : '☀️ Világos'), 'info');
};

window.applyTheme = function(theme) {
  var root = document.documentElement;
  root.setAttribute('data-theme', theme);
  var vars = theme === 'light' ? {
    '--bg': '#f5f5f5', '--surface': '#ffffff', '--surface2': '#e9ecef',
    '--text': '#212529', '--text2': '#495057', '--text3': '#6c757d',
    '--border': '#dee2e6', '--primary': '#0d6efd', '--primary-dim': 'rgba(13,110,253,0.1)',
    '--success': '#198754', '--warning': '#ffc107', '--danger': '#dc3545'
  } : {
    '--bg': '#0d1117', '--surface': '#161b22', '--surface2': '#21262d',
    '--text': '#e6edf3', '--text2': '#b1bac4', '--text3': '#7d8590',
    '--border': '#30363d', '--primary': '#58a6ff', '--primary-dim': 'rgba(88,166,255,0.1)',
    '--success': '#3fb950', '--warning': '#d29922', '--danger': '#f85149'
  };
  var style = document.getElementById('theme-vars');
  if (!style) {
    style = document.createElement('style');
    style.id = 'theme-vars';
    document.head.appendChild(style);
  }
  var css = ':root{';
  for (var k in vars) { css += k + ':' + vars[k] + ';'; }
  css += '}';
  style.textContent = css;
};

// Init theme on load
(function() {
  var saved = localStorage.getItem('a2a_theme') || 'dark';
  applyTheme(saved);
})();

// ─── Kanban Drag-and-Drop ──
var dragData = null;

window.handleDragStart = function(e) {
  dragData = {
    cardId: e.target.getAttribute('data-card-id'),
    boardId: e.target.getAttribute('data-board-id'),
    fromColumn: e.target.getAttribute('data-column')
  };
  e.target.style.opacity = '0.5';
  e.dataTransfer.effectAllowed = 'move';
};

window.handleDragEnd = function(e) {
  e.target.style.opacity = '1';
  dragData = null;
  // Remove highlight from all columns
  var cols = document.querySelectorAll('.kanban-col-drop');
  for (var i = 0; i < cols.length; i++) {
    cols[i].style.background = '';
  }
};

window.handleDragOver = function(e) {
  e.preventDefault();
  e.dataTransfer.dropEffect = 'move';
  if (e.currentTarget) {
    e.currentTarget.style.background = 'rgba(88,166,255,0.08)';
  }
};

window.handleDragLeave = function(e) {
  if (e.currentTarget) {
    e.currentTarget.style.background = '';
  }
};

window.handleDrop = function(e) {
  e.preventDefault();
  e.stopPropagation();
  var targetColumn = e.currentTarget.getAttribute('data-column');
  if (e.currentTarget) {
    e.currentTarget.style.background = '';
  }
  if (!dragData || !targetColumn) return;
  if (dragData.fromColumn === targetColumn) return;

  var token = localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || '';
  var boardId = dragData.boardId || 'main';
  var cardId = dragData.cardId;

  fetch('/api/kanban/' + boardId + '/cards/' + cardId, {
    method: 'PUT',
    headers: { 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json' },
    body: JSON.stringify({ column: targetColumn })
  })
    .then(function(r) { return r.json(); })
    .then(function(d) {
      if (d.error) {
        showToast('Hiba: ' + d.error, 'error');
      } else {
        showToast('Card mozgatva: ' + dragData.fromColumn + ' → ' + targetColumn, 'success');
        loadMarveenPage('kanban');
      }
    })
    .catch(function(e) { showToast('Drag-drop hiba: ' + e, 'error'); });
};

window.initKanbanDnD = function() {
  // Make cards draggable
  var cards = document.querySelectorAll('[data-card-id]');
  for (var i = 0; i < cards.length; i++) {
    cards[i].setAttribute('draggable', 'true');
    cards[i].addEventListener('dragstart', handleDragStart);
    cards[i].addEventListener('dragend', handleDragEnd);
  }
  // Make columns droppable
  var cols = document.querySelectorAll('[data-column]');
  for (var j = 0; j < cols.length; j++) {
    cols[j].classList.add('kanban-col-drop');
    cols[j].addEventListener('dragover', handleDragOver);
    cols[j].addEventListener('dragleave', handleDragLeave);
    cols[j].addEventListener('drop', handleDrop);
  }
};

// ─── Multi-API section loaders (for projects, network, security, sysinfo)
window._fetchSection = function(url, targetId, renderFn) {
  var token = localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || '';
  fetch(url, { headers: { 'Authorization': 'Bearer ' + token } })
    .then(function(r) { return r.json(); })
    .then(function(d) {
      var el = document.getElementById(targetId);
      if (el) el.innerHTML = renderFn(d);
    })
    .catch(function(e) {
      var el = document.getElementById(targetId);
      if (el) el.innerHTML = '<div style="color:var(--text3);font-size:10px;">Hiba: ' + e.message + '</div>';
    });
};

// Projects: team + workflows
window._loadProjectsExtras = function() {
  window._fetchSection('/api/team', 'projects-team-section', function(d) {
    var nodes = d.nodes || [];
    if (!nodes.length) return '';
    var h = '<h3 style="margin:0 0 8px;font-size:13px;">👥 Team</h3>';
    nodes.forEach(function(n) {
      h += '<div style="background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:10px;margin-bottom:6px;">';
      h += '<div style="display:flex;align-items:center;gap:6px;"><strong style="font-size:12px;">' + esc(n.name || '?') + '</strong>';
      h += '<span style="font-size:9px;padding:1px 6px;border-radius:4px;background:var(--primary)22;color:var(--primary);">' + esc(n.role || '?') + '</span></div>';
      if (n.delegates_to && n.delegates_to.length) h += '<div style="font-size:10px;color:var(--text3);margin-top:2px;">Delegál: ' + esc(n.delegates_to.join(', ')) + '</div>';
      if (n.reports_to) h += '<div style="font-size:10px;color:var(--text3);">Jelent: ' + esc(n.reports_to) + '</div>';
      h += '</div>';
    });
    return h;
  });
  window._fetchSection('/api/workflows', 'projects-workflows-section', function(d) {
    var wfs = d.workflows || [];
    var h = '<h3 style="margin:0 0 8px;font-size:13px;">📋 Workflow-k (' + (d.total || wfs.length) + ')</h3>';
    h += '<div style="margin-bottom:6px;"><button onclick="showWorkflowCreateModal()" style="background:var(--primary);color:#fff;border:none;padding:4px 10px;border-radius:4px;cursor:pointer;font-size:11px;">➕ Új workflow</button></div>';
    if (!wfs.length) { h += '<div style="color:var(--text3);font-size:10px;">Nincs workflow</div>'; return h; }
    wfs.slice(0, 10).forEach(function(w) {
      var stColor = w.status === 'completed' ? 'var(--success)' : w.status === 'running' ? 'var(--primary)' : 'var(--text3)';
      var wid = w.id || w._id || '';
      h += '<div style="background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:10px;margin-bottom:6px;">';
      h += '<div style="display:flex;align-items:center;gap:6px;"><span style="font-size:9px;padding:1px 6px;border-radius:4px;background:' + stColor + '22;color:' + stColor + ';">' + esc(w.status || '?') + '</span><strong style="font-size:12px;flex:1;">' + esc(w.name || w.id) + '</strong><span style="font-size:10px;color:var(--text3);">' + esc(w.tasks || 0) + ' feladat</span><button onclick="workflowDelete(\'' + esc(wid) + '\')" style="font-size:9px;background:rgba(239,68,68,.2);color:var(--danger);border:1px solid var(--danger);padding:2px 6px;border-radius:4px;cursor:pointer;">🗑️</button></div>';
      h += '</div>';
    });
    return h;
  });
};

// Network: MCP end-devices list + watchdog + desired-state + channel-health
window._loadMCPNetwork = function() {
  var box = document.getElementById('network-mcp-section');
  if (!box) return;
  var token = localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || '';
  fetch('/api/mesh/topology', {headers: {'Authorization': 'Bearer ' + token}})
    .then(function(r) { return r.json(); })
    .then(function(d) {
      var nodes = (d && d.nodes) || [];
      var mcpEnds = nodes.filter(function(n) { return n.is_mcp_end_device; });
      var html = '<h3 style="margin:0 0 8px;font-size:13px;color:#bc8cff;">🔌 MCP End Device-ek (' + mcpEnds.length + ')</h3>';
      if (!mcpEnds.length) {
        html += '<div style="background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:12px;font-size:11px;color:var(--text3);text-align:center;">Nincs MCP-n csatlakozó agent</div>';
        box.innerHTML = html;
        return;
      }
      mcpEnds.forEach(function(n) {
        var st = n.status || 'idle';
        var stColor = st === 'online' ? 'var(--success)' : st === 'idle' ? 'var(--warning)' : 'var(--text3)';
        var age = n.last_seen ? fmtAgo(n.last_seen) : '—';
        html += '<div style="background:var(--surface);border:1px solid #bc8cff44;border-radius:8px;padding:10px;margin-bottom:6px;">';
        html += '<div style="display:flex;align-items:center;gap:8px;"><span style="font-size:14px;">🔌</span><strong style="font-size:12px;flex:1;">' + esc(n.name) + '</strong><span style="width:8px;height:8px;border-radius:50%;background:' + stColor + ';"></span><span style="font-size:10px;color:var(--text3);">' + esc(st) + '</span></div>';
        html += '<div style="font-size:10px;color:var(--text3);margin-top:4px;">Parent: <strong>' + esc(n.transport_parent || '?') + '</strong> • Transport: MCP • Utolsó: ' + age + '</div>';
        html += '</div>';
      });
      box.innerHTML = html;
    })
    .catch(function() { box.innerHTML = ''; });
};

function fmtAgo(ts) {
  if (!ts) return '—';
  var d = new Date((typeof ts === 'number' && ts < 1e12) ? ts * 1000 : ts);
  if (isNaN(d)) return String(ts);
  var diff = (Date.now() - d.getTime()) / 1000;
  if (diff < 60) return 'most';
  if (diff < 3600) return Math.floor(diff / 60) + ' perce';
  if (diff < 86400) return Math.floor(diff / 3600) + ' órája';
  return Math.floor(diff / 86400) + ' napja';
}

window._loadNetworkExtras = function() {
  window._loadMCPNetwork();
  window._fetchSection('/api/watchdog/status', 'network-watchdog-section', function(d) {
    var nodes = d.monitored_nodes || [];
    var th = d.thresholds || {};
    var h = '<h3 style="margin:0 0 8px;font-size:13px;">🐕 Watchdog</h3>';
    h += '<div style="background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:10px;margin-bottom:6px;font-size:10px;color:var(--text3);">';
    h += 'Warning: ' + esc(th.warning_s || '?') + 's • Busy: ' + esc(th.busy_s || '?') + 's • Stuck: ' + esc(th.stuck_s || '?') + 's • Dead: ' + esc(th.dead_s || '?') + 's';
    h += '</div>';
    if (nodes.length) {
      nodes.forEach(function(n) {
        var stColor = n.state === 'ok' ? 'var(--success)' : n.state === 'stuck' ? 'var(--danger)' : 'var(--warning)';
        h += '<div style="background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:8px;margin-bottom:4px;display:flex;align-items:center;gap:6px;"><div style="width:8px;height:8px;border-radius:50%;background:' + stColor + ';"></div><strong style="font-size:12px;flex:1;">' + esc(n.name || n.node || '?') + '</strong><span style="font-size:10px;color:var(--text3);">' + esc(n.state || '?') + '</span></div>';
      });
    } else {
      h += '<div style="color:var(--text3);font-size:10px;text-align:center;padding:8px;">Nincs monitorozott node</div>';
    }
    return h;
  });
  window._fetchSection('/api/desired-state', 'network-desired-section', function(d) {
    var dn = d.desired_nodes || {};
    var keys = Object.keys(dn);
    if (!keys.length) return '';
    var h = '<h3 style="margin:0 0 8px;font-size:13px;">🎯 Desired State</h3>';
    h += '<div style="display:flex;gap:6px;margin-bottom:6px;"><button onclick="showDesiredStateModal()" style="background:var(--primary);color:#fff;border:none;padding:4px 10px;border-radius:4px;cursor:pointer;font-size:10px;">➕ Add</button></div>';
    keys.forEach(function(k) {
      var n = dn[k];
      var enColor = n.enabled ? 'var(--success)' : 'var(--text3)';
      h += '<div style="background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:8px;margin-bottom:4px;display:flex;align-items:center;gap:6px;"><span style="font-size:9px;padding:2px 6px;border-radius:4px;background:' + enColor + '22;color:' + enColor + ';">' + (n.enabled ? 'ON' : 'OFF') + '</span><strong style="font-size:12px;flex:1;">' + esc(k) + '</strong><button onclick="desiredStateRemove(\'' + esc(k) + '\')" style="font-size:9px;background:rgba(239,68,68,.2);color:var(--danger);border:1px solid var(--danger);padding:2px 6px;border-radius:4px;cursor:pointer;">✕</button></div>';
    });
    return h;
  });
  window._fetchSection('/api/channel-health', 'network-channel-section', function(d) {
    var channels = d.channels || {};
    var keys = Object.keys(channels);
    if (!keys.length) return '';
    var h = '<h3 style="margin:0 0 8px;font-size:13px;">📡 Channel Health</h3>';
    keys.forEach(function(k) {
      var c = channels[k];
      var stColor = c.status === 'ok' ? 'var(--success)' : c.status === 'error' ? 'var(--danger)' : 'var(--text3)';
      h += '<div style="background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:8px;margin-bottom:4px;display:flex;align-items:center;gap:6px;"><div style="width:8px;height:8px;border-radius:50%;background:' + stColor + ';"></div><strong style="font-size:12px;flex:1;">' + esc(k) + '</strong></div>';
    });
    return h;
  });
};

// Security: memory-boundary + login-throttle + process-lock + recovery-notes
window._loadSecurityExtras = function() {
  window._fetchSection('/api/memory-boundary', 'security-memory-section', function(d) {
    var nodes = d.nodes || [];
    if (!nodes.length) return '';
    var h = '<h3 style="margin:0 0 8px;font-size:13px;">🧠 Memory Boundary</h3>';
    nodes.forEach(function(n) {
      h += '<div style="background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:8px;margin-bottom:4px;">';
      h += '<div style="display:flex;align-items:center;gap:6px;"><strong style="font-size:12px;flex:1;">' + esc(n.name || '?') + '</strong><span style="font-size:10px;color:var(--text3);">' + esc(n.shared_count || 0) + ' shared</span></div>';
      if (n.shared && n.shared.length) h += '<div style="font-size:10px;color:var(--text3);margin-top:2px;">' + esc(n.shared.join(', ')) + '</div>';
      h += '</div>';
    });
    return h;
  });
  window._fetchSection('/api/login-throttle', 'security-throttle-section', function(d) {
    var locked = d.locked_users || [];
    var h = '<h3 style="margin:0 0 8px;font-size:13px;">🔐 Login Throttle</h3>';
    h += '<div style="background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:10px;margin-bottom:6px;font-size:11px;color:var(--text3);">';
    h += 'Failures/hour: <strong style="color:var(--text);">' + esc(d.global_failures_hour || 0) + '</strong> / ' + esc(d.global_threshold || 50) + ' • Max/user: ' + esc(d.max_failures_per_user || 5);
    h += '</div>';
    if (locked.length) {
      h += '<div style="color:var(--danger);font-size:11px;">Zárolt: ' + esc(locked.join(', ')) + '</div>';
    } else {
      h += '<div style="color:var(--success);font-size:10px;text-align:center;padding:4px;">✅ Nincs zárolt felhasználó</div>';
    }
    return h;
  });
  window._fetchSection('/api/process-lock', 'security-lock-section', function(d) {
    var h = '<h3 style="margin:0 0 8px;font-size:13px;">🔒 Process Lock</h3>';
    var acqColor = d.acquired ? 'var(--success)' : 'var(--text3)';
    h += '<div style="background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:10px;font-size:11px;color:var(--text3);">';
    h += 'Acquired: <strong style="color:' + acqColor + ';">' + (d.acquired ? 'Igen' : 'Nem') + '</strong> • Port: ' + esc(d.port || '—') + ' • PID: ' + esc(d.pid || '—');
    h += '</div>';
    return h;
  });
  window._fetchSection('/api/recovery-notes', 'security-recovery-section', function(d) {
    var notes = d.notes || [];
    var h = '<h3 style="margin:0 0 8px;font-size:13px;">📝 Recovery Notes</h3>';
    h += '<div style="margin-bottom:8px;"><button onclick="showRecoveryNoteModal()" style="background:var(--primary);color:#fff;border:none;padding:6px 12px;border-radius:6px;cursor:pointer;font-size:11px;">➕ Új jegyzet</button></div>';
    if (!notes.length) {
      h += '<div style="color:var(--text3);font-size:10px;text-align:center;padding:8px;">Nincs recovery note</div>';
      return h;
    }
    var h = '<h3 style="margin:0 0 8px;font-size:13px;">📝 Recovery Notes</h3>';
    notes.slice(0, 10).forEach(function(n) {
      var nid = n.id || '';
      h += '<div style="background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:8px;margin-bottom:4px;">';
      h += '<div style="display:flex;align-items:center;gap:6px;"><strong style="font-size:11px;">' + esc(n.target_node || '?') + '</strong><span style="font-size:9px;color:var(--text3);">' + esc(n.author || '?') + '</span>';
      if (!n.read) h += '<button onclick="recoveryNoteRead(' + esc(nid) + ')" style="font-size:9px;background:var(--surface2);border:1px solid var(--border);padding:2px 6px;border-radius:4px;cursor:pointer;margin-left:auto;">✓ Olvasva</button>';
      h += '</div>';
      h += '<div style="font-size:10px;color:var(--text3);margin-top:2px;">' + esc((n.note || '').substring(0, 120)) + '</div>';
      h += '</div>';
    });
    return h;
  });
};

// Session management
window._loadSessionInfo = function() {
  var token = localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || '';
  fetch('/api/auth/session-timeout', { headers: { 'Authorization': 'Bearer ' + token } })
    .then(function(r) { return r.json(); })
    .then(function(d) {
      var input = document.getElementById('sessionTimeoutInput');
      if (input && d.timeout_hours !== undefined) input.value = d.timeout_hours;
    }).catch(function(e) {});
  fetch('/api/auth/sessions', { headers: { 'Authorization': 'Bearer ' + token } })
    .then(function(r) { return r.json(); })
    .then(function(d) {
      var container = document.getElementById('sessionListContainer');
      if (!container) return;
      var sessions = d.sessions || [];
      if (!sessions.length) { container.innerHTML = '<div style="font-size:11px;color:var(--text3);">Nincs aktív session.</div>'; return; }
      var h = '<div style="font-size:12px;font-weight:600;margin-bottom:6px;color:var(--text2);">Aktív sessions (' + sessions.length + '):</div>';
      sessions.forEach(function(s) {
        var exp = s.expires_at ? new Date(s.expires_at * 1000).toLocaleString('hu-HU') : '\u221e soha nem j\u00e1r le';
        var created = s.created_at ? new Date(s.created_at * 1000).toLocaleString('hu-HU') : '?';
        h += '<div style="display:flex;align-items:center;gap:6px;padding:6px 8px;background:var(--surface2);border-radius:6px;margin-bottom:4px;font-size:11px;">';
        h += '<span style="flex:1;"><strong>' + esc(s.username || '?') + '</strong> \u2014 ' + esc(exp) + ' <span style="color:var(--text3);">(l\u00e9trehozva: ' + esc(created) + ')</span></span>';
        h += '<button onclick="revokeSession(this)" data-token="' + esc(s.token) + '" style="background:var(--danger);color:#fff;border:none;padding:4px 10px;border-radius:4px;cursor:pointer;font-size:10px;">Visszavon</button>';
        h += '</div>';
      });
      container.innerHTML = h;
    }).catch(function(e) {});
};

window.saveSessionTimeout = function() {
  var input = document.getElementById('sessionTimeoutInput');
  if (!input) return;
  var timeout = parseFloat(input.value) || 0;
  var token = localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || '';
  fetch('/api/auth/session-timeout', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + token },
    body: JSON.stringify({ timeout_hours: timeout })
  }).then(function(r) { return r.json(); })
    .then(function(d) {
      if (d.ok) { alert('Session timeout be\u00e1ll\u00edtva: ' + (timeout === 0 ? 'v\u00e9gtelen \u221e' : timeout + ' \u00f3ra') + '\n\n\u00dajra be kell jelentkezned a v\u00e1ltoztat\u00e1s \u00e9rv\u00e9nyes\u00fcl\u00e9s\u00e9hez!'); }
      else { alert('Hiba: ' + (d.error || 'ismeretlen')); }
      window._loadSessionInfo();
    }).catch(function(e) { alert('Hiba: ' + e.message); });
};

window.revokeSession = function(btn) {
  var tokenSig = btn.getAttribute('data-token');
  if (!confirm('Biztosan visszavonod ezt a session-t?')) return;
  var token = localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || '';
  fetch('/api/auth/revoke-session', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + token },
    body: JSON.stringify({ token: tokenSig })
  }).then(function(r) { return r.json(); })
    .then(function(d) { if (d.ok) window._loadSessionInfo(); else alert('Hiba: ' + (d.error || 'ismeretlen')); })
    .catch(function(e) { alert('Hiba: ' + e.message); });
};

// SysInfo: queue + retries + store + workers + model + db + labels + voice
window._loadSysinfoExtras = function() {
  window._fetchSection('/api/queue/stats', 'sysinfo-queue-section', function(d) {
    var ls = d.local_store || {};
    var h = '<h3 style="margin:0 0 8px;font-size:13px;">📬 Queue Stats</h3>';
      html += '<div style="display:flex;gap:8px;margin-bottom:12px;flex-wrap:wrap;">';
      html += '<button onclick="sysinfoQueueAction(\'flush\')" style="background:var(--warning);color:#000;border:none;padding:6px 12px;border-radius:6px;cursor:pointer;font-size:11px;">🗑️ Flush</button>';
      html += '<button onclick="sysinfoQueueAction(\'cleanup\')" style="background:var(--surface2);color:var(--text);border:1px solid var(--border);padding:6px 12px;border-radius:6px;cursor:pointer;font-size:11px;">🧹 Cleanup</button>';
      html += '<button onclick="debugLog()" style="background:var(--surface2);color:var(--text);border:1px solid var(--border);padding:6px 12px;border-radius:6px;cursor:pointer;font-size:11px;">📝 Debug Log</button>';
      html += '<button onclick="modelFallbackError()" style="background:var(--surface2);color:var(--text);border:1px solid var(--border);padding:6px 12px;border-radius:6px;cursor:pointer;font-size:11px;">⚠️ Model Error</button>';
      html += '<button onclick="routeCalc()" style="background:var(--surface2);color:var(--text);border:1px solid var(--border);padding:6px 12px;border-radius:6px;cursor:pointer;font-size:11px;">🔀 Route Calc</button>';
      html += '<button onclick="sendAgentMessage()" style="background:var(--primary);color:#fff;border:none;padding:6px 12px;border-radius:6px;cursor:pointer;font-size:11px;">📤 Agent Msg</button>';
      html += '<button onclick="agentReply()" style="background:var(--primary);color:#fff;border:none;padding:6px 12px;border-radius:6px;cursor:pointer;font-size:11px;">↩️ Agent Reply</button>';
      html += '</div>';
    h += '<div style="display:flex;gap:8px;flex-wrap:wrap;">';
    var items = [
      {l: 'Outbound pending', v: ls.outbound_pending || 0, c: 'var(--warning)'},
      {l: 'Outbound synced', v: ls.outbound_synced || 0, c: 'var(--success)'},
      {l: 'Inbound unprocessed', v: ls.inbound_unprocessed || 0, c: 'var(--danger)'},
      {l: 'Files pending', v: ls.files_pending || 0, c: 'var(--text3)'}
    ];
    items.forEach(function(s) {
      h += '<div style="background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:6px 10px;text-align:center;min-width:80px;"><div style="font-size:14px;font-weight:700;color:' + s.c + ';">' + esc(s.v) + '</div><div style="font-size:9px;color:var(--text3);">' + esc(s.l) + '</div></div>';
    });
    h += '</div>';
    return h;
  });
  window._fetchSection('/api/pending-retries', 'sysinfo-retries-section', function(d) {
    var stats = d.stats || {};
    var h = '<h3 style="margin:0 0 8px;font-size:13px;">🔄 Pending Retries</h3>';
    h += '<div style="background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:10px;font-size:11px;color:var(--text3);">';
    h += 'Total: <strong style="color:var(--text);">' + esc(stats.total || 0) + '</strong> • Pending: ' + esc(stats.pending || 0) + ' • Alerting: ' + esc(stats.alerting || 0);
    h += '</div>';
    return h;
  });
  window._fetchSection('/api/store-watcher', 'sysinfo-store-section', function(d) {
    var inv = d.inventory || {};
    var h = '<h3 style="margin:0 0 8px;font-size:13px;">📦 Store Watcher</h3>';
    h += '<div style="background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:10px;font-size:11px;color:var(--text3);">';
    h += 'Files: <strong style="color:var(--text);">' + esc(inv.total_files || 0) + '</strong> • Size: ' + esc(Math.round((inv.total_size_bytes || 0) / 1024)) + 'KB • Agent: ' + esc(inv.agent_files || 0) + ' • System: ' + esc(inv.system_files || 0);
    h += '</div>';
    return h;
  });
  window._fetchSection('/api/worker-liveness', 'sysinfo-workers-section', function(d) {
    var workers = d.workers || [];
    var issues = d.issues || [];
    var h = '<h3 style="margin:0 0 8px;font-size:13px;">👷 Worker Liveness</h3>';
    h += '<div style="background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:10px;font-size:11px;color:var(--text3);">';
    h += 'Workers: <strong style="color:var(--text);">' + esc(d.count || workers.length) + '</strong> • Issues: ' + esc(issues.length);
    h += '</div>';
    return h;
  });
  window._fetchSection('/api/model-fallback/nova', 'sysinfo-model-section', function(d) {
    var h = '<h3 style="margin:0 0 8px;font-size:13px;">🤖 Model Fallback</h3>';
    h += '<div style="background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:10px;margin-bottom:6px;">';
    h += '<div style="font-size:12px;"><strong>Current:</strong> ' + esc(d.current_model || '?') + ' • Position: ' + esc(d.chain_position || 0) + '</div>';
    if (d.chain && d.chain.length) {
      h += '<div style="font-size:10px;color:var(--text3);margin-top:4px;">Chain: ' + esc(d.chain.join(' → ')) + '</div>';
    }
    if (d.total_errors > 0) {
      h += '<div style="font-size:10px;color:var(--danger);margin-top:4px;">Errors: ' + esc(d.total_errors) + '</div>';
    }
    h += '</div>';
    return h;
  });
  window._fetchSection('/api/marveen-db/status', 'sysinfo-db-section', function(d) {
    var tables = d.tables || {};
    var keys = Object.keys(tables);
    if (!keys.length) return '';
    var h = '<h3 style="margin:0 0 8px;font-size:13px;">🗄️ Marveen DB (' + esc(d.total_tables || keys.length) + ' tables)</h3>';
    keys.forEach(function(k) {
      h += '<div style="background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:6px 10px;margin-bottom:3px;display:flex;align-items:center;gap:6px;"><strong style="font-size:11px;flex:1;">' + esc(k) + '</strong><span style="font-size:12px;font-weight:700;color:var(--primary);">' + esc(tables[k]) + '</span></div>';
    });
    return h;
  });
  window._fetchSection('/api/labels', 'sysinfo-labels-section', function(d) {
    var labels = d.labels || [];
    var h = '<h3 style="margin:0 0 8px;font-size:13px;">🏷️ Labels</h3>';
    h += '<div style="margin-bottom:6px;"><button onclick="showLabelAddModal()" style="background:var(--primary);color:#fff;border:none;padding:4px 10px;border-radius:4px;cursor:pointer;font-size:11px;">➕ Új label</button></div>';
    if (!labels.length) { h += '<div style="color:var(--text3);font-size:10px;">Nincs label</div>'; return h; }
    var h = '<h3 style="margin:0 0 8px;font-size:13px;">🏷️ Labels</h3>';
    labels.forEach(function(l) {
      h += '<span style="display:inline-block;font-size:10px;padding:3px 8px;border-radius:4px;background:' + esc(l.color || 'var(--primary)') + '22;color:' + esc(l.color || 'var(--primary)') + ';margin:2px;">' + esc(l.name || '?') + '</span>';
    });
    return h;
  });
  window._fetchSection('/api/voice', 'sysinfo-voice-section', function(d) {
    var h = '<h3 style="margin:0 0 8px;font-size:13px;">🎤 Voice</h3>';
    h += '<div style="display:flex;gap:6px;margin-bottom:6px;"><button onclick="voiceParse()" style="background:var(--primary);color:#fff;border:none;padding:4px 10px;border-radius:4px;cursor:pointer;font-size:10px;">🎤 Parse</button><button onclick="promptSafetyCheck()" style="background:var(--surface2);color:var(--text);border:1px solid var(--border);padding:4px 10px;border-radius:4px;cursor:pointer;font-size:10px;">🛡️ Safety Check</button></div>';
    h += '<div style="background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:10px;font-size:11px;color:var(--text3);">';
    h += 'Enabled: <strong style="color:' + (d.enabled ? 'var(--success)' : 'var(--text3)') + ';">' + (d.enabled ? 'Igen' : 'Nem') + '</strong> • Languages: ' + esc((d.languages || []).join(', ')) + ' • Directives: ' + esc(d.directive_count || 0);
    h += '</div>';
    return h;
  });
};

// Hook into page loader to trigger extras
(function() {
  var origLoad = window.loadMarveenPage;
  if (origLoad && !origLoad._hooked) {
    window.loadMarveenPage = function(page) {
      origLoad(page);
      setTimeout(function() {
        if (page === 'projects') window._loadProjectsExtras();
        else if (page === 'network') window._loadNetworkExtras();
        else if (page === 'security') { window._loadSecurityExtras(); window._loadSessionInfo(); }
        else if (page === 'sysinfo') window._loadSysinfoExtras();
        else if (page === 'health') window._loadHealthExtras();
        else if (page === 'vault') window._loadVaultExtras();
      }, 100);
    };
    window.loadMarveenPage._hooked = true;
  }
})();

// ── Vault extras: mesh áttekintés + helyi bejegyzések automatikus betöltése ──
window._loadVaultExtras = function() {
  var token = window._vaultToken();
  // Mesh áttekintés: agent-bar + node kártyák + helyi node bejegyzései
  window.vaultShowMeshOverview(function(meshNodes) {
    // Helyi node megkeresése a mesh válaszból
    var localName = null;
    Object.keys(meshNodes || {}).forEach(function(n) {
      if ((meshNodes[n] || {}).local) localName = n;
    });
    if (!localName) return;
    window._vaultCurrentAgent = localName;
    fetch('/api/vault/remote/' + encodeURIComponent(localName), { headers: { 'Authorization': 'Bearer ' + token } })
      .then(function(r) { return r.json(); })
      .then(function(d) {
        if (d && d.entries) {
          window._vaultCurrentAgentIsLocal = !!d.local;
          window._vaultRenderEntries(d.entries, d.node || localName, !!d.local);
        }
      })
      .catch(function() {});
  });
};

window._vaultRefresh = function() {
  if (window._vaultCurrentAgent) window.vaultSelectAgent(window._vaultCurrentAgent);
  else window._loadVaultExtras();
};

// ── Health extras: node erőforrások + P2P hálózat ──
window._loadHealthExtras = function() {
  function _hb(text, color) {
    var c = color || 'var(--primary)';
    return '<span style="display:inline-block;padding:2px 8px;border-radius:10px;font-size:11px;font-weight:600;background:' + c + '22;color:' + c + ';">' + String(text == null ? '' : text).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;') + '</span>';
  }
  // Node CPU/mem/disk
  window._fetchSection('/api/health/nodes', 'health-nodes-section', function(d) {
    var nodes = d.nodes || [];
    if (!Array.isArray(nodes) && typeof nodes === 'object') nodes = Object.keys(nodes).map(function(k) { var n = nodes[k] || {}; n.node_name = n.node_name || k; return n; });
    var h = '<h3 style="margin:0 0 8px;font-size:13px;color:var(--text2);">🖥️ Node erőforrások</h3>';
    if (!nodes.length) return h + '<div style="color:var(--text3);font-size:11px;">Nincs node adat</div>';
    h += '<div style="display:flex;flex-direction:column;gap:6px;">';
    nodes.forEach(function(n) {
      function bar(pct, warn) {
        var color = pct >= 90 ? 'var(--danger)' : pct >= (warn || 70) ? 'var(--warning)' : 'var(--success)';
        return '<div style="display:flex;align-items:center;gap:6px;">' +
          '<div style="flex:1;height:8px;background:var(--surface2);border-radius:4px;overflow:hidden;min-width:60px;"><div style="width:' + Math.min(pct, 100) + '%;height:100%;background:' + color + ';border-radius:4px;"></div></div>' +
          '<span style="font-size:10px;color:var(--text2);min-width:36px;text-align:right;">' + (pct >= 100 ? '100' : Math.round(pct)) + '%</span></div>';
      }
      var stColor = n.status === 'active' ? 'var(--success)' : 'var(--danger)';
      h += '<div style="background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:10px 12px;">' +
        '<div style="display:flex;align-items:center;gap:8px;margin-bottom:6px;">' +
          '<strong style="font-size:12px;">' + esc(n.node_name || '?') + '</strong>' +
          _hb(n.status || '?', stColor) +
          '<span style="font-size:9px;color:var(--text3);margin-left:auto;">frissítve: ' + esc(n.last_seen ? n.last_seen.substring(11, 16) : '?') + '</span>' +
        '</div>' +
        '<div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:10px;font-size:9px;color:var(--text3);">' +
          '<div>CPU' + bar(n.cpu_pct || 0, 80) + '</div>' +
          '<div>MEM' + bar(n.memory_pct || 0, 80) + '</div>' +
          '<div>DISK' + bar(n.disk_pct || 0, 85) + '</div>' +
        '</div>' +
      '</div>';
    });
    h += '</div>';
    return h;
  });
  // P2P hálózat
  window._fetchSection('/api/p2p/status', 'health-p2p-section', function(d) {
    var peers = d.peers || [];
    var backoff = d.backoff_peers || [];
    var h = '<h3 style="margin:0 0 8px;font-size:13px;color:var(--text2);">📡 P2P hálózat</h3>';
    h += '<div style="background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:10px 12px;font-size:11px;color:var(--text3);">';
    h += '<div style="margin-bottom:6px;display:flex;gap:10px;flex-wrap:wrap;">' +
      '<span>Port: <strong style="color:var(--text);">' + esc(d.listen_port || '?') + '</strong></span>' +
      '<span>TLS: <strong style="color:' + (d.tls_enabled ? 'var(--success)' : 'var(--danger)') + ';">' + (d.tls_enabled ? '✅ aktív' : '❌ nincs') + '</strong></span>' +
      '<span>Peerek: <strong style="color:var(--text);">' + esc(d.peer_count || peers.length) + '</strong></span>' +
      '<span>Bejövő sor: <strong style="color:var(--text);">' + esc(d.incoming_queue || 0) + '</strong></span>' +
    '</div>';
    h += '<div style="display:flex;gap:6px;flex-wrap:wrap;">';
    if (peers.length) {
      peers.forEach(function(p) { h += _hb('🟢 ' + esc(p), 'var(--success)'); });
    } else { h += '<span style="color:var(--text3);">Nincs kapcsolódott peer</span>'; }
    h += '</div>';
    if (backoff && backoff.length) {
      h += '<div style="margin-top:6px;font-size:10px;color:var(--danger);">Backoff: ' + backoff.map(function(b) { return esc(b); }).join(', ') + '</div>';
    }
    h += '</div>';
    return h;
  });
};

// ─── Settings Functions ──────────────────────────────────
function showSettings() {
  document.getElementById('settingsModal').style.display = 'flex';
  loadSettings();
  loadSettingsNodes();
  loadAlertRules();
  loadUsers();
}

// ── Delegation UI ──

function showDelegations() {
  document.getElementById('delegationsModal').style.display = 'flex';
  populateDelegationTargets();
  loadDelegations();
  loadDelegationStats();
  startDelegationAutoRefresh();
}

var _delegationAutoRefresh = null;
function startDelegationAutoRefresh() {
  if (_delegationAutoRefresh) clearInterval(_delegationAutoRefresh);
  _delegationAutoRefresh = setInterval(function() {
    var modal = document.getElementById('delegationsModal');
    if (!modal || modal.style.display === 'none') {
      clearInterval(_delegationAutoRefresh);
      _delegationAutoRefresh = null;
      return;
    }
    loadDelegations();
    loadDelegationStats();
  }, 5000);
}

// Known agents cache for eligible agents panel
var _knownAgents = [];

function populateDelegationTargets() {
  var sel = document.getElementById('delTarget');
  while (sel.options.length > 1) sel.remove(1);
  fetch("/api/agents", {headers: {"Authorization": "Bearer " + (localStorage.getItem("a2a_token") || localStorage.getItem("mesh_token") || "")}})
    .then(function(r) { return r.json(); })
    .then(function(data) {
      var agents = data.agents || data || [];
      _knownAgents = [];
      agents.forEach(function(a) {
        if (a.name && a.name !== nodeName) {
          _knownAgents.push(a);
          var opt = document.createElement("option");
          opt.value = a.name;
          opt.textContent = a.name + (a.status === "online" ? " 🟢" : " 🟡");
          sel.appendChild(opt);
        }
      });
    });
}

function toggleEligibleAgents() {
  var checked = document.getElementById("delAvailable").checked;
  var panel = document.getElementById("eligibleAgentsPanel");
  var targetSel = document.getElementById("delTarget");
  if (checked) {
    targetSel.disabled = true;
    targetSel.value = "";
    panel.style.display = "block";
    var list = document.getElementById("eligibleAgentsList");
    list.innerHTML = "";
    _knownAgents.forEach(function(a) {
      var label = document.createElement("label");
      label.style.cssText = "display:flex;align-items:center;gap:4px;padding:4px 8px;background:var(--surface2);border-radius:6px;cursor:pointer;font-size:12px;border:1px solid var(--border)";
      var cb = document.createElement("input");
      cb.type = "checkbox";
      cb.value = a.name;
      cb.className = "eligible-agent-cb";
      cb.checked = true;
      cb.style.cursor = "pointer";
      var dot = a.status === "online" ? "🟢" : "🟡";
      var skills = (a.skills && a.skills.length) ? a.skills.slice(0, 2).join(", ") : "";
      label.appendChild(cb);
      label.appendChild(document.createTextNode(dot + " " + a.name + (skills ? " (" + skills + ")" : "")));
      list.appendChild(label);
    });
    if (_knownAgents.length === 0) {
      list.innerHTML = '<span style="color:var(--text3);font-size:12px">Nincs elérhető agent</span>';
    }
  } else {
    targetSel.disabled = false;
    panel.style.display = "none";
  }
}

function toggleNewTask() {
  var f = document.getElementById('newTaskForm');
  f.style.display = f.style.display === 'none' ? 'block' : 'none';
  if (f.style.display === 'block') {
    populateDelegationTargets();
    document.getElementById("delAvailable").checked = false;
    document.getElementById("eligibleAgentsPanel").style.display = "none";
    document.getElementById("delTarget").disabled = false;
  }
}

function renderKanbanCard(card, delegation) {
  var isDelegation = !!delegation;
  var typeClass = "type-generic";
  var typeLabel = "📋 Kanban";
  
  if (delegation) {
    typeClass = {"code":"type-code","research":"type-research","analysis":"type-analysis","monitoring":"type-monitoring"}[delegation.task_type] || "type-generic";
    typeLabel = {"code":"💻 Kód","research":"🔍 Kutatás","analysis":"📊 Elemzés","monitoring":"📈 Monitor"}[delegation.task_type] || "🔧 Általános";
  }
  
  var prio = delegation ? (delegation.priority || card.priority || 5) : (card.priority || 5);
  var prioClass = "priority-" + prio;
  var prioLabel = "P" + prio;
  
  var created_at = delegation ? delegation.created_at : card.created_at;
  var timeAgo = created_at ? timeSince(new Date(created_at)) : "?";
  
  var assigned = delegation && delegation.assigned_agent ? "👤" + escHtml(delegation.assigned_agent) : "";
  var fromTo = delegation ? "📤" + escHtml(delegation.from_agent || "?") + " → " + (delegation.to_agent === "any" ? "🔄Bárki" : "📥" + escHtml(delegation.to_agent || "?")) : "";
  
  var progressHtml = "";
  if (delegation && delegation.progress) {
    var pColor = delegation.progress >= 80 ? "var(--success)" : delegation.progress >= 40 ? "var(--warning)" : "var(--info)";
    progressHtml = '<div class="card-progress"><div class="card-progress-bar" style="width:' + delegation.progress + '%;background:' + pColor + '"></div></div><span style="font-size:10px;color:var(--text3)">' + delegation.progress + '%</span>';
  }
  
  var notesHtml = "";
  if (delegation && delegation.notes && Array.isArray(delegation.notes) && delegation.notes.length > 0) {
    var ln = delegation.notes[delegation.notes.length - 1];
    notesHtml = '<div style="font-size:10px;color:var(--text3);margin-top:3px;padding:3px 5px;background:var(--surface);border-radius:3px">📝 ' + escHtml((ln.agent || "?") + ": " + (ln.note || "")).substring(0, 60) + '</div>';
  }
  
  var resultHtml = "";
  var resultText = delegation ? delegation.result : card.delegation_result;
  if (resultText) {
    resultHtml = '<div class="card-result">' + escHtml(resultText).substring(0, 120) + '</div>';
  }
  
  var actions = '<div class="card-actions">';
  if (delegation) {
    if (delegation.status === "available") {
      actions += '<button style="background:var(--success);color:#fff" onclick="claimDelegation(\'' + delegation.task_id + '\')">✋ Vállalom</button>';
    }
    if (delegation.status === "pending" || delegation.status === "accepted") {
      actions += '<button style="background:var(--danger);color:#fff" onclick="cancelDelegation(\'' + delegation.task_id + '\')">🚫</button>';
    }
    if (delegation.status === "accepted" || delegation.status === "running") {
      actions += '<button style="background:var(--info);color:#fff" onclick="reassignDelegation(\'' + delegation.task_id + '\')">🔄</button>';
      actions += '<button style="background:var(--text3);color:#fff" onclick="addDelegationNote(\'' + delegation.task_id + '\')">📝</button>';
    }
    if (delegation.status === "running") {
      actions += '<button style="background:var(--warning);color:#000" onclick="updateDelegationProgress(\'' + delegation.task_id + '\')">📊</button>';
    }
    if (delegation.status === "completed" && delegation.result_files && delegation.result_files.length > 0) {
       actions += '<button style="background:var(--primary);color:#fff" onclick="showTaskDetail(\'' + delegation.task_id + '\')">⬇️ Letöltés</button>';
    }
  }
  actions += '</div>';
  
  var badge = isDelegation ? '<span style="font-size:12px">🔗</span>' : '<span style="font-size:12px">📋</span>';
  var clickAction = isDelegation ? "showTaskDetail('" + delegation.task_id + "')" : "showKanbanCardDetail('" + card.id + "')";
  
  return '<div class="kanban-card" data-card-id="' + card.id + '" data-column="' + (card.column || 'todo') + '" data-board-id="' + (card.board_id || 'main') + '" style="cursor:pointer" onclick="' + clickAction + '">' +
    '<div class="card-subject"><span>' + escHtml(card.title || (delegation ? delegation.subject : "?")) + '</span>' + badge + '<span class="priority-badge ' + prioClass + '">' + prioLabel + '</span></div>' +
    '<div><span class="card-type ' + typeClass + '">' + typeLabel + '</span></div>' +
    '<div class="card-meta">' + (fromTo ? '<span>' + fromTo + '</span>' : '') + (assigned ? '<span>' + assigned + '</span>' : '') + '<span>' + timeAgo + '</span></div>' +
    progressHtml + notesHtml + resultHtml + actions +
    '</div>';
}

var _loadDelegationsRunning = false;
function loadDelegations() {
  if (_loadDelegationsRunning) return;
  _loadDelegationsRunning = true;
  
  var filters = "";
  var fs = document.getElementById("delFilterStatus");
  var ft = document.getElementById("delFilterType");
  var fp = document.getElementById("delFilterPriority");
  var fa = document.getElementById("delFilterAgent");
  
  if (fs && fs.value) filters += "&status=" + encodeURIComponent(fs.value);
  if (ft && ft.value) filters += "&task_type=" + encodeURIComponent(ft.value);
  if (fa && fa.value.trim()) filters += "&agent=" + encodeURIComponent(fa.value.trim());
  
  var token = localStorage.getItem("a2a_token") || localStorage.getItem("mesh_token") || "";
  
  Promise.all([
    fetch("/api/kanban", {headers: {"Authorization": "Bearer " + token}}).then(function(r) { return r.json(); }),
    fetch("/api/delegations?limit=100" + filters, {headers: {"Authorization": "Bearer " + token}}).then(function(r) { return r.json(); })
  ]).then(function(results) {
    var kanbanData = results[0];
    var delData = results[1];
    var delegations = delData.delegations || [];
    var kanbanCards = [];
    
    if (kanbanData.boards && kanbanData.boards.length > 0) {
      kanbanCards = kanbanData.boards[0].cards || [];
    }
    
    var enrichedCards = kanbanCards.map(function(card) {
      var del = delegations.find(function(d) { return d.task_id === card.delegation_task_id; });
      if (del) {
        if (del.status === 'completed' || del.status === 'cancelled' || del.status === 'failed' || del.status === 'expired') card.column = 'done';
        else if (del.status === 'running' || del.status === 'accepted') card.column = 'in_progress';
        else if (del.status === 'available' || del.status === 'pending') card.column = 'todo';
      }
      return { card: card, delegation: del };
    });
    
    delegations.forEach(function(d) {
      if (!kanbanCards.find(function(c) { return c.delegation_task_id === d.task_id; })) {
        enrichedCards.push({
          card: { id: d.task_id, title: d.subject, column: (d.status === 'completed' || d.status === 'cancelled' || d.status === 'failed' || d.status === 'expired' ? 'done' : d.status === 'running' || d.status === 'accepted' ? 'in_progress' : 'todo'), priority: d.priority, created_at: d.created_at },
          delegation: d
        });
      }
    });
    
    // Clear columns
    ["todo", "in_progress", "review", "done"].forEach(function(s) {
      var col = document.getElementById("kanban-" + s);
      if (col) col.innerHTML = "";
    });
    
    var counts = {todo:0, in_progress:0, review:0, done:0};
    
    // Apply priority filter
    if (fp && fp.value) {
      var minP = parseInt(fp.value);
      enrichedCards = enrichedCards.filter(function(item) { return (item.delegation ? item.delegation.priority : item.card.priority) === minP; });
    }
    
    var fcEl = document.getElementById("delFilterCount");
    if (fcEl) fcEl.textContent = enrichedCards.length + " feladat";
    
    if (enrichedCards.length === 0) {
      ["todo", "in_progress", "review", "done"].forEach(function(s) {
        var col = document.getElementById("kanban-" + s);
        if (col) col.innerHTML = '<div style="text-align:center;color:var(--text3);font-size:11px;padding:16px">Üres</div>';
      });
      _loadDelegationsRunning = false;
      return;
    }
    
    // Sort: done by completion time (newest first), others by creation time
    enrichedCards.sort(function(a, b) {
      var aCol = a.card.column || "todo";
      var bCol = b.card.column || "todo";
      if (aCol === "done" && bCol === "done") {
        var aTime = (a.delegation && (a.delegation.completed_at || a.delegation.updated_at)) || a.card.created_at || "";
        var bTime = (b.delegation && (b.delegation.completed_at || b.delegation.updated_at)) || b.card.created_at || "";
        return String(bTime).localeCompare(String(aTime));
      }
      var aTime2 = (a.delegation && a.delegation.created_at) || a.card.created_at || "";
      var bTime2 = (b.delegation && b.delegation.created_at) || b.card.created_at || "";
      return String(bTime2).localeCompare(String(aTime2));
    });
    
    // Render cards
    enrichedCards.forEach(function(item) {
      var col = item.card.column || "todo";
      if (["todo", "in_progress", "review", "done"].indexOf(col) === -1) col = "todo";
      var el = document.getElementById("kanban-" + col);
      if (el) {
        try {
          el.innerHTML += renderKanbanCard(item.card, item.delegation);
          counts[col]++;
        } catch(e) {
          console.error("renderKanbanCard error:", e);
        }
      }
    });
    
    // Update column headers
    ["todo", "in_progress", "review", "done"].forEach(function(s) {
      var header = document.querySelector('[data-status="' + s + '"] .kanban-col-header');
      if (header) {
        var baseText = header.textContent.replace(/\s*\(\d+\)$/, '');
        header.textContent = baseText.trim() + ' (' + counts[s] + ')';
      }
    });
    
    loadDelegationStats();
  }).catch(function(e) {
    console.error("Load delegations error:", e);
  }).finally(function() {
    _loadDelegationsRunning = false;
  });
}
function loadDelegationStats() {
  fetch("/api/delegations/stats", {headers: {"Authorization": "Bearer " + (localStorage.getItem("a2a_token") || localStorage.getItem("mesh_token") || "")}})
    .then(function(r) { return r.json(); })
    .then(function(data) {
      var el = document.getElementById("delStats");
      var total = data.total || 0;
      var items = [
        data.available ? '<span style="color:var(--info)">🔄' + data.available + ' elérhető</span>' : '',
        data.pending ? '<span style="color:#8b5cf6">⏳' + (data.pending || 0) + ' függő</span>' : '',
        data.accepted ? '<span style="color:#8b5cf6">✅' + data.accepted + ' elfogadott</span>' : '',
        data.running ? '<span style="color:var(--warning)">⚙️' + data.running + ' futó</span>' : '',
        data.completed ? '<span style="color:var(--success)">✔️' + data.completed + ' kész</span>' : '',
        data.failed ? '<span style="color:var(--danger)">❌' + data.failed + ' hiba</span>' : ''
      ].filter(Boolean);
      el.innerHTML = items.length ? '<b>Összesen: ' + total + '</b> · ' + items.join(' · ') : 'Még nincs delegáció';
    });
}

function showTaskDetail(taskId) {
  // Hide delegations modal background so it doesn't block the detail modal
  var delModal = document.getElementById('delegationsModal');
  if (delModal) delModal.style.display = 'none';
  document.getElementById('taskDetailModal').style.display = 'flex';
  document.getElementById('taskDetailTitle').textContent = '📋 Betöltés...';
  document.getElementById('taskDetailContent').innerHTML = '<div style="text-align:center;padding:32px;color:var(--text3)">Betöltés...</div>';
  fetch("/api/delegations/" + taskId, {headers: {"Authorization": "Bearer " + (localStorage.getItem("a2a_token") || localStorage.getItem("mesh_token") || "")}})
    .then(function(r) { return r.json(); })
    .then(function(d) {
      if (d.error) { document.getElementById('taskDetailContent').innerHTML = '<div style="color:var(--danger)">❌ ' + escHtml(d.error) + '</div>'; return; }
      var statusColors = {available:"var(--info)",pending:"#8b5cf6",accepted:"#8b5cf6",running:"var(--warning)",completed:"var(--success)",failed:"var(--danger)",cancelled:"var(--danger)",expired:"var(--danger)"};
      var statusIcons = {available:"🔄",pending:"⏳",accepted:"✅",running:"⚙️",completed:"✔️",failed:"❌",cancelled:"🚫",expired:"⌛"};
      var sColor = statusColors[d.status] || "var(--text3)";
      var sIcon = statusIcons[d.status] || "❓";
      document.getElementById('taskDetailTitle').textContent = sIcon + " " + escHtml(d.subject || "?");

      // Parse description
      var descHtml = "";
      if (d.description) {
        try {
          var descObj = typeof d.description === "string" ? JSON.parse(d.description) : d.description;
          var descText = descObj.description || descObj.type || "";
          if (descText) descHtml = '<div style="margin-bottom:12px;padding:10px;background:var(--surface2);border-radius:8px;font-size:13px;color:var(--text2);line-height:1.5">' + escHtml(descText) + '</div>';
        } catch(e) {
          descHtml = '<div style="margin-bottom:12px;padding:10px;background:var(--surface2);border-radius:8px;font-size:13px;color:var(--text2)">' + escHtml(str(d.description).substring(0, 300)) + '</div>';
        }
      }

      // Timeline from notes
      var timelineHtml = "";
      if (d.notes && Array.isArray(d.notes) && d.notes.length > 0) {
        timelineHtml = '<div style="margin-bottom:12px"><h4 style="margin:0 0 8px 0;font-size:13px">📝 Idővonal</h4>';
        d.notes.forEach(function(n) {
          var t = n.time ? new Date(n.time).toLocaleTimeString("hu-HU", {hour:"2-digit",minute:"2-digit"}) : "?";
          var agent = n.agent || "?";
          timelineHtml += '<div style="display:flex;gap:8px;margin-bottom:6px;font-size:12px">' +
            '<span style="color:var(--text3);min-width:42px">' + t + '</span>' +
            '<span style="font-weight:600;color:var(--info);min-width:50px">' + escHtml(agent) + '</span>' +
            '<span style="color:var(--text2)">' + escHtml(n.note || "").substring(0, 200) + '</span></div>';
        });
        timelineHtml += '</div>';
      }

      // Result
      var resultHtml = "";
      if (d.result) {
        resultHtml = '<div style="margin-bottom:12px"><h4 style="margin:0 0 8px 0;font-size:13px">📄 Eredmény</h4>' +
          '<div style="padding:10px;background:var(--surface2);border-radius:8px;font-size:12px;color:var(--text2);max-height:150px;overflow-y:auto;white-space:pre-wrap;line-height:1.4">' + escHtml(d.result).substring(0, 2000) + '</div></div>';
      }

      // File download — fetch files list dynamically
      var fileHtml = "";
      if (d.result_file || d.status === "completed") {
        var token = localStorage.getItem("a2a_token") || localStorage.getItem("mesh_token") || "";
        var filesUrl = "/api/delegations/" + d.task_id + "/files?token=" + encodeURIComponent(token);
        fileHtml = '<div style="margin-bottom:12px" id="taskFiles"><h4 style="margin:0 0 8px 0;font-size:13px">📎 Eredmény fájlok</h4><div style="color:var(--text3);font-size:12px">Betöltés...</div></div>';
        // Fetch files after modal renders
        setTimeout(function() {
          fetch(filesUrl, {headers: {"Authorization": "Bearer " + token}})
            .then(function(r) { return r.json(); })
            .then(function(data) {
              var container = document.getElementById("taskFiles");
              if (!container) return;
              var files = data.files || [];
              if (files.length === 0) {
                container.innerHTML = '<h4 style="margin:0 0 8px 0;font-size:13px">📎 Eredmény fájlok</h4><div style="color:var(--text3);font-size:12px">Nincs csatolt fájl</div>';
                return;
              }
              var baseUrl = "/api/delegations/" + d.task_id + "/files?token=" + encodeURIComponent(token);
              var html = '<h4 style="margin:0 0 8px 0;font-size:13px">📎 Eredmény fájlok (' + files.length + ')</h4>';
              html += '<div style="display:flex;flex-direction:column;gap:6px">';
              files.forEach(function(f) {
                var icon = f.filename.endsWith(".py") ? "🐍" : f.filename.endsWith(".html") ? "🌐" : f.filename.endsWith(".pptx") ? "📊" : f.filename.endsWith(".zip") ? "📦" : f.filename.endsWith(".pdf") ? "📕" : "📄";
                var sizeKB = f.file_size ? Math.round(f.file_size / 1024) + " KB" : "?";
                var dlUrl = baseUrl + "&download=1&file_id=" + encodeURIComponent(f.id);
                html += '<div style="display:flex;align-items:center;gap:8px;padding:8px;background:var(--surface2);border-radius:8px;border:1px solid var(--border)">';
                html += '<span style="font-size:18px">' + icon + '</span>';
                html += '<div style="flex:1;min-width:0">';
                html += '<div style="font-size:13px;font-weight:600;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">' + escHtml(f.filename) + '</div>';
                html += '<div style="font-size:11px;color:var(--text3)">' + sizeKB + ' · ' + escHtml(f.content_type || "text/plain") + '</div>';
                if (f.preview) html += '<div style="font-size:10px;color:var(--text3);margin-top:2px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">' + escHtml(f.preview.substring(0, 120)) + '</div>';
                html += '</div>';
                html += '<a href="' + dlUrl + '" target="_blank" class="btn btn-sm" style="background:var(--success);color:#fff;text-decoration:none;white-space:nowrap">⬇️ Letöltés</a>';
                html += '</div>';
              });
              if (files.length > 1) {
                var zipUrl = baseUrl + "&zip=1";
                html += '<a href="' + zipUrl + '" target="_blank" class="btn btn-sm" style="background:var(--info);color:#fff;text-decoration:none;display:inline-flex;align-items:center;gap:4px;margin-top:4px">📦 Összes letöltése ZIP-ként</a>';
              }
              html += '</div>';
              container.innerHTML = html;
            })
            .catch(function(e) {
              var container = document.getElementById("taskFiles");
              if (container) container.innerHTML = '<h4 style="margin:0 0 8px 0;font-size:13px">📎 Eredmény fájlok</h4><div style="color:var(--danger);font-size:12px">Hiba: ' + escHtml(e.message || String(e)) + '</div>';
            });
        }, 100);
      }

      // Meta info
      var metaHtml = '<div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-bottom:12px;font-size:12px">';
      metaHtml += '<div><span style="color:var(--text3)">Státusz:</span> <span style="color:' + sColor + ';font-weight:600">' + sIcon + ' ' + d.status + '</span></div>';
      metaHtml += '<div><span style="color:var(--text3)">Prioritás:</span> <span class="priority-badge priority-' + (d.priority || 5) + '">P' + (d.priority || 5) + '</span></div>';
      metaHtml += '<div><span style="color:var(--text3)">Feladó:</span> 📤 ' + escHtml(d.from_agent || "?") + '</div>';
      var toLabel = d.to_agent === "any" ? "🔄 Bárki" : "📥 " + escHtml(d.to_agent || "?");
      metaHtml += '<div><span style="color:var(--text3)">Címzett:</span> ' + toLabel + '</div>';
      if (d.assigned_agent) metaHtml += '<div><span style="color:var(--text3)">Feldolgozó:</span> 👤 ' + escHtml(d.assigned_agent) + '</div>';
      var typeLabel = {"code":"💻 Kódolás","research":"🔍 Kutatás","analysis":"📊 Elemzés","monitoring":"📈 Monitorozás"}[d.task_type] || "🔧 Általános";
      metaHtml += '<div><span style="color:var(--text3)">Típus:</span> ' + typeLabel + '</div>';
      if (d.progress) metaHtml += '<div><span style="color:var(--text3)">Haladás:</span> ' + d.progress + '%</div>';
      if (d.created_at) {
        var created = new Date(d.created_at);
        metaHtml += '<div><span style="color:var(--text3)">Létrehozva:</span> ' + created.toLocaleString("hu-HU") + '</div>';
      }
      if (d.completed_at) {
        var completed = new Date(d.completed_at);
        metaHtml += '<div><span style="color:var(--text3)">Befejezve:</span> ' + completed.toLocaleString("hu-HU") + '</div>';
      }
      if (d.created_at && d.completed_at) {
        var dur = Math.round((new Date(d.completed_at) - new Date(d.created_at)) / 1000);
        var durStr = dur < 60 ? dur + "mp" : dur < 3600 ? Math.round(dur/60) + "perc" : Math.round(dur/3600*10)/10 + "óra";
        metaHtml += '<div><span style="color:var(--text3)">Időtartam:</span> ⏱️ ' + durStr + '</div>';
      }
      // Deadline countdown
      if (d.created_at && d.timeout_minutes && d.status !== "completed" && d.status !== "failed" && d.status !== "cancelled" && d.status !== "expired") {
        var created = new Date(d.created_at);
        var deadline = new Date(created.getTime() + d.timeout_minutes * 60000);
        var now = new Date();
        var remaining = deadline - now;
        if (remaining > 0) {
          var mins = Math.floor(remaining / 60000);
          var secs = Math.floor((remaining % 60000) / 1000);
          var dColor = mins < 5 ? "var(--danger)" : mins < 15 ? "var(--warning)" : "var(--success)";
          metaHtml += '<div style="grid-column:1/-1"><span style="color:var(--text3)">Határidő:</span> <span style="color:' + dColor + ';font-weight:600">⏰ ' + mins + 'p ' + secs + 'mp</span></div>';
        } else {
          metaHtml += '<div style="grid-column:1/-1"><span style="color:var(--text3)">Határidő:</span> <span style="color:var(--danger);font-weight:600">⏰ Lejárt!</span></div>';
        }
      }
      metaHtml += '</div>';

      // Progress bar
      var progressHtml = "";
      if (d.progress && d.status !== "completed") {
        var pColor = d.progress >= 80 ? "var(--success)" : d.progress >= 40 ? "var(--warning)" : "var(--info)";
        progressHtml = '<div style="margin-bottom:12px"><div style="background:var(--surface);border-radius:6px;height:8px;overflow:hidden"><div style="height:100%;width:' + d.progress + '%;background:' + pColor + ';border-radius:6px;transition:width .3s"></div></div><div style="font-size:11px;color:var(--text3);text-align:center;margin-top:2px">' + d.progress + '% kész</div></div>';
      }

      // Actions
      var actionsHtml = '<div style="display:flex;gap:8px;flex-wrap:wrap;margin-top:8px">';
      if (d.status === "available") actionsHtml += '<button class="btn btn-sm" style="background:var(--success);color:#fff" onclick="claimDelegation(\'' + d.task_id + '\');showTaskDetail(\'' + d.task_id + '\')">✋ Vállalom</button>';
      if (d.status === "pending" || d.status === "accepted") actionsHtml += '<button class="btn btn-sm" style="background:var(--danger);color:#fff" onclick="cancelDelegation(\'' + d.task_id + '\');showTaskDetail(\'' + d.task_id + '\')">🚫 Mégsem</button>';
      if (d.status === "accepted" || d.status === "running") {
        actionsHtml += '<button class="btn btn-sm" style="background:var(--info);color:#fff" onclick="reassignDelegation(\'' + d.task_id + '\');showTaskDetail(\'' + d.task_id + '\')">🔄 Átcélzó</button>';
        actionsHtml += '<button class="btn btn-sm" style="background:var(--text3);color:#fff" onclick="addDelegationNote(\'' + d.task_id + '\');showTaskDetail(\'' + d.task_id + '\')">📝 Jegyzet</button>';
      }
      if (d.status === "running") actionsHtml += '<button class="btn btn-sm" style="background:var(--warning);color:#000" onclick="updateDelegationProgress(\'' + d.task_id + '\');showTaskDetail(\'' + d.task_id + '\')">📊 Haladás</button>';
      if (d.status === "cancelled" || d.status === "completed" || d.status === "expired") {
        actionsHtml += '<button class="btn btn-sm" style="background:var(--danger);color:#fff" onclick="deleteDelegation(\'' + d.task_id + '\')">🗑️ Törlés</button>';
        actionsHtml += '<button class="btn btn-sm" style="background:var(--success);color:#fff" onclick="redispatchDelegation(\'' + d.task_id + '\')">🔄 Újra kiosztás</button>';
      }
      actionsHtml += '<button class="btn btn-sm" style="background:var(--surface2);color:var(--text)" onclick="showTaskDetail(\'' + d.task_id + '\')">🔄 Frissítés</button>';
      actionsHtml += '</div>';

      document.getElementById('taskDetailContent').innerHTML = metaHtml + descHtml + progressHtml + timelineHtml + resultHtml + fileHtml + actionsHtml;
    })
    .catch(function(e) {
      document.getElementById('taskDetailContent').innerHTML = '<div style="color:var(--danger)">❌ Hiba: ' + escHtml(e.message || String(e)) + '</div>';
    });
}

function claimDelegation(taskId) {
  if (!confirm("Vállalod ezt a feladatot?")) return;
  fetch("/api/delegations/" + taskId + "/claim", {
    method: "POST",
    headers: {"Content-Type": "application/json", "Authorization": "Bearer " + (localStorage.getItem("a2a_token") || localStorage.getItem("mesh_token") || "")}
  })
  .then(function(r) { return r.json(); })
  .then(function(data) {
    if (data.error) { alert("Hiba: " + data.error); } else { alert("✋ Feladat vállalva: " + data.claimed_by); loadDelegations(); }
  });
}

function reassignDelegation(taskId) {
  var agent = prompt("Új agent neve (morzsa / runa / nova):");
  if (!agent) return;
  fetch("/api/delegations/" + taskId + "/reassign", {
    method: "POST",
    headers: {"Content-Type": "application/json", "Authorization": "Bearer " + (localStorage.getItem("a2a_token") || localStorage.getItem("mesh_token") || "")},
    body: JSON.stringify({agent: agent})
  })
  .then(function(r) { return r.json(); })
  .then(function(data) {
    if (data.error) { alert("Hiba: " + data.error); } else { alert("🔄 Átcélozva: " + data.assigned_to); loadDelegations(); }
  });
}

function addDelegationNote(taskId) {
  var note = prompt("Jegyzet:");
  if (!note) return;
  fetch("/api/delegations/" + taskId + "/note", {
    method: "POST",
    headers: {"Content-Type": "application/json", "Authorization": "Bearer " + (localStorage.getItem("a2a_token") || localStorage.getItem("mesh_token") || "")},
    body: JSON.stringify({note: note})
  })
  .then(function(r) { return r.json(); })
  .then(function(data) {
    if (data.error) { alert("Hiba: " + data.error); } else { loadDelegations(); }
  });
}

function updateDelegationProgress(taskId) {
  var pct = prompt("Haladás (0-100):", "50");
  if (!pct) return;
  var note = prompt("Opcionális jegyzet:");
  fetch("/api/delegations/" + taskId + "/progress", {
    method: "POST",
    headers: {"Content-Type": "application/json", "Authorization": "Bearer " + (localStorage.getItem("a2a_token") || localStorage.getItem("mesh_token") || "")},
    body: JSON.stringify({progress: parseInt(pct), note: note || undefined})
  })
  .then(function(r) { return r.json(); })
  .then(function(data) {
    if (data.error) { alert("Hiba: " + data.error); } else { alert("📊 Haladás: " + data.progress + "%"); loadDelegations(); }
  });
}

function createDelegation() {
  var target = document.getElementById('delTarget').value;
  var subject = document.getElementById('delSubject').value;
  var desc = document.getElementById('delDesc').value;
  var type = document.getElementById('delType').value;
  var priority = parseInt(document.getElementById('delPriority').value);
  var available = document.getElementById('delAvailable').checked;
  var statusEl = document.getElementById('delCreateStatus');

  if (!subject) {
    statusEl.textContent = "❌ Tárgy kötelező!";
    statusEl.style.color = "var(--danger)";
    return;
  }
  if (!available && !target) {
    statusEl.textContent = "❌ Válassz cél-agentet vagy jelöld be 'Bárki vállalhatja'!";
    statusEl.style.color = "var(--danger)";
    return;
  }

  statusEl.textContent = "📤 Küldés...";
  statusEl.style.color = "var(--text3)";

  var fanOut = parseInt(document.getElementById('delFanOut').value) || 0;
  var distributeMode = document.getElementById('delDistributeMode').value === 'true';
  var dependsOn = document.getElementById('delDependsOn').value.trim();
  
  var body = {subject: subject, description: desc, task_type: type, priority: priority};
  if (fanOut > 0) body.fan_out = fanOut;
  if (distributeMode && fanOut > 0) body.distribute_mode = true;
  if (dependsOn) body.depends_on = dependsOn;
  if (available) {
    body.to_agent = "any";
    body.available = true;
    // Collect eligible agents from checkboxes
    var eligible = [];
    document.querySelectorAll('.eligible-agent-cb:checked').forEach(function(cb) {
      eligible.push(cb.value);
    });
    if (eligible.length > 0 && eligible.length < _knownAgents.length) {
      body.eligible_agents = eligible;
    }
  } else {
    body.to_agent = target;
  }

  fetch("/api/delegations", {
    method: "POST",
    headers: {"Content-Type": "application/json", "Authorization": "Bearer " + (localStorage.getItem("a2a_token") || localStorage.getItem("mesh_token") || "")},
    body: JSON.stringify(body)
  })
  .then(function(r) { return r.json(); })
  .then(function(data) {
    if (data.task_id) {
      statusEl.textContent = "✅ Elküldve! ID: " + data.task_id.substring(0, 8);
      statusEl.style.color = "var(--success)";
      document.getElementById('delSubject').value = "";
      document.getElementById('delDesc').value = "";
      loadDelegations();
      loadDelegationStats();
    } else {
      statusEl.textContent = "❌ " + (data.error || "Hiba");
      statusEl.style.color = "var(--danger)";
    }
  })
  .catch(function(e) {
    statusEl.textContent = "❌ Hálózati hiba";
    statusEl.style.color = "var(--danger)";
  });
}

function cancelDelegation(taskId) {
  fetch("/api/delegations/" + taskId + "/cancel", {
    method: "POST",
    headers: {"Authorization": "Bearer " + (localStorage.getItem("mesh_token") || "")}
  })
  .then(function(r) { return r.json(); })
  .then(function(data) {
    loadDelegations();
    loadDelegationStats();
  });
}

function deleteDelegation(taskId) {
  if (!confirm("Biztosan törlöd ezt a feladatot? Ez végleges!")) return;
  fetch("/api/delegations/" + taskId, {
    method: "DELETE",
    headers: {"Authorization": "Bearer " + (localStorage.getItem("a2a_token") || localStorage.getItem("mesh_token") || "")}
  })
  .then(function(r) { return r.json(); })
  .then(function(data) {
    if (data.error) { alert("Hiba: " + data.error); }
    else {
      document.getElementById("taskDetailModal").style.display = "none";
      var dm = document.getElementById("delegationsModal");
      if (dm) dm.style.display = "flex";
      loadDelegations();
      loadDelegationStats();
    }
  })
  .catch(function(e) { alert("Hiba: " + e); });
}

function redispatchDelegation(taskId) {
  if (!confirm("Újra kiosztod ezt a feladatot?")) return;
  fetch("/api/delegations/" + taskId + "/redispatch", {
    method: "POST",
    headers: {"Authorization": "Bearer " + (localStorage.getItem("a2a_token") || localStorage.getItem("mesh_token") || "")}
  })
  .then(function(r) { return r.json(); })
  .then(function(data) {
    if (data.error) { alert("Hiba: " + data.error); }
    else {
      alert("✅ Újra kiosztva: " + data.new_task_id);
      if (data.new_task_id) showTaskDetail(data.new_task_id);
      loadDelegations();
      loadDelegationStats();
    }
  })
  .catch(function(e) { alert("Hiba: " + e); });
}

function timeSince(date) {
  var seconds = Math.floor((new Date() - date) / 1000);
  if (seconds < 60) return seconds + "s";
  var minutes = Math.floor(seconds / 60);
  if (minutes < 60) return minutes + "p";
  var hours = Math.floor(minutes / 60);
  if (hours < 24) return hours + "ó";
  var days = Math.floor(hours / 24);
  return days + "n";
}

function escHtml(str) {
  var div = document.createElement('div');
  div.textContent = str || '';
  return div.innerHTML;
}

function loadSettings() {
  fetch("/api/settings", {headers: {"Authorization": "Bearer " + (localStorage.getItem("mesh_token") || "")}})
    .then(function(r) { return r.json(); })
    .then(function(data) {
      document.getElementById("settingsMeshInfo").innerHTML =
        "<b>Node:</b> " + data.mesh.node_name + "<br>" +
        "<b>P2P:</b> " + (data.mesh.p2p_enabled ? "✅ Engedélyezve" : "❌ Letiltva") + "<br>" +
        "<b>PG:</b> " + (data.mesh.pg_enabled ? "✅ Engedélyezve" : "❌ Letiltva") + "<br>" +
        "<b>Agentek:</b> " + data.registry.total_agents + " regisztrált, " + data.registry.pending_agents + " függő";
      document.getElementById("settingsAutoApprove").checked = data.registry.auto_approve;
      document.getElementById("settingsAutoApproveLabel").textContent = data.registry.auto_approve ? "Bekapcsolva" : "Kikapcsolva";
      document.getElementById("settingsHealthInterval").value = data.registry.health_check_interval;
      document.getElementById("settingsWeightLatency").value = data.health_scorer.weights.latency;
      document.getElementById("settingsWeightSuccess").value = data.health_scorer.weights.success_rate;
      document.getElementById("settingsWeightAvail").value = data.health_scorer.weights.availability;
      document.getElementById("settingsLatencyThreshold").value = data.health_scorer.latency_threshold_ms;
      document.getElementById("settingsDecayFactor").value = data.health_scorer.decay_factor;
      document.getElementById("settingsRecoveryFactor").value = data.health_scorer.recovery_factor;
      loadPendingAgents();
      loadTransportStatus();
      loadSharedConfig();
    })
    .catch(function(e) { console.error("Settings load error:", e); });
}

// ── Transport Status ──
function loadTransportStatus() {
  // Use /health (node endpoint, no auth needed) for real transport data
  fetch("/health")
    .then(function(r) { return r.json(); })
    .then(function(data) {
      var t = data.transports || {};
      var ssh = data.ssh_tunnel || {};
      var p2p = data.p2p || {};
      var html = "<div style='display:grid;grid-template-columns:1fr 1fr 1fr 1fr;gap:6px;margin-bottom:8px'>";
      // P2P card
      html += "<div style='text-align:center;padding:6px;border-radius:6px;background:" + (t.p2p ? "var(--success)" : "var(--danger)") + "22'><div style='font-size:18px'>" + (t.p2p ? "✅" : "❌") + "</div><div style='font-size:10px'>P2P</div></div>";
      // PG card
      html += "<div style='text-align:center;padding:6px;border-radius:6px;background:" + (t.pg ? "var(--success)" : "var(--danger)") + "22'><div style='font-size:18px'>" + (t.pg ? "✅" : "❌") + "</div><div style='font-size:10px'>PG</div></div>";
      // SSH Tunnel card
      html += "<div style='text-align:center;padding:6px;border-radius:6px;background:" + (t.ssh_tunnel ? "var(--success)" : "var(--danger)") + "22'><div style='font-size:18px'>" + (t.ssh_tunnel ? "✅" : "❌") + "</div><div style='font-size:10px'>SSH Tunnel</div></div>";
      // HTTP card
      html += "<div style='text-align:center;padding:6px;border-radius:6px;background:" + (t.http ? "var(--success)" : "var(--danger)") + "22'><div style='font-size:18px'>" + (t.http ? "✅" : "❌") + "</div><div style='font-size:10px'>HTTP</div></div>";
      html += "</div>";

      // P2P details
      if (p2p && p2p.peers && p2p.peers.length > 0) {
        html += "<div style='font-size:11px;margin-bottom:6px'><b>P2P:</b> port " + (p2p.listen_port || 8645) + ", TLS " + (p2p.tls_enabled ? "✅" : "❌") + ", peers: ";
        html += p2p.peers.map(escHtml).join(", ") + "</div>";
      }

      // SSH tunnel peers table
      if (ssh && Object.keys(ssh).length > 0) {
        html += "<table style='width:100%;border-collapse:collapse;font-size:11px;margin-top:4px'>";
        html += "<tr style='border-bottom:1px solid var(--border)'><th style='text-align:left;padding:3px'>Peer</th><th style='text-align:left;padding:3px'>Státusz</th><th style='text-align:left;padding:3px'>Host</th><th style='text-align:right;padding:3px'>Port</th><th style='text-align:right;padding:3px'>Uptime</th></tr>";
        for (var peer in ssh) {
          if (ssh.hasOwnProperty(peer)) {
            var p = ssh[peer];
            var conn = p.connected ? "🟢 Connected" : "🔴 Disconnected";
            var up = p.uptime_seconds ? Math.round(p.uptime_seconds) + "s" : "-";
            var retry = p.retry_count > 0 ? " (retry #" + p.retry_count + ")" : "";
            html += "<tr style='border-bottom:1px solid var(--border)'>";
            html += "<td style='padding:3px'><b>" + escHtml(peer) + "</b></td>";
            html += "<td style='padding:3px'>" + conn + retry + "</td>";
            html += "<td style='padding:3px;font-size:10px;color:var(--text3)'>" + escHtml(p.ssh_host || "-") + "</td>";
            html += "<td style='padding:3px;text-align:right;font-size:10px'>" + (p.local_port || "-") + "→" + (p.remote_port || "-") + "</td>";
            html += "<td style='padding:3px;text-align:right'>" + up + "</td>";
            html += "</tr>";
          }
        }
        html += "</table>";
      } else if (t.ssh_tunnel) {
        html += "<div style='font-size:11px;color:var(--text3);margin-top:4px'>SSH tunnel engedélyezve, de nincs aktív peer kapcsolat</div>";
      }

      document.getElementById("settingsTransport").innerHTML = html;
    })
    .catch(function(e) {
      document.getElementById("settingsTransport").innerHTML = "<span style='color:var(--danger)'>Hiba: " + escHtml(e.message) + "</span>";
    });
}

// ── Shared Config Loading ──
function loadSharedConfig() {
  fetch("/api/config/shared", {headers: {"Authorization": "Bearer " + (localStorage.getItem("mesh_token") || "")}})
    .then(function(r) { return r.json(); })
    .then(function(data) {
      var cfg = data.config || {};
      // Delegation
      if (cfg["delegation.expiry_minutes"]) document.getElementById("cfgDelExpiry").value = cfg["delegation.expiry_minutes"].value;
      if (cfg["delegation.auto_renew"]) {
        document.getElementById("cfgDelAutoRenew").checked = cfg["delegation.auto_renew"].value;
        document.getElementById("cfgDelAutoRenewLabel").textContent = cfg["delegation.auto_renew"].value ? "Bekapcsolva" : "Kikapcsolva";
      }
      if (cfg["delegation.cpu_threshold_p4"]) document.getElementById("cfgDelCpuP4").value = cfg["delegation.cpu_threshold_p4"].value;
      if (cfg["delegation.cpu_threshold_p7"]) document.getElementById("cfgDelCpuP7").value = cfg["delegation.cpu_threshold_p7"].value;
      // Security
      if (cfg["security.rate_limit_per_minute"]) document.getElementById("cfgSecRateLimit").value = cfg["security.rate_limit_per_minute"].value;
      if (cfg["security.token_rotation_interval"]) document.getElementById("cfgSecTokenRot").value = cfg["security.token_rotation_interval"].value;
      if (cfg["security.mtls_enabled"]) {
        document.getElementById("cfgSecMtls").checked = cfg["security.mtls_enabled"].value;
        document.getElementById("cfgSecMtlsLabel").textContent = cfg["security.mtls_enabled"].value ? "Bekapcsolva" : "Kikapcsolva";
      }
      if (cfg["security.hmac_enabled"]) {
        document.getElementById("cfgSecHmac").checked = cfg["security.hmac_enabled"].value;
        document.getElementById("cfgSecHmacLabel").textContent = cfg["security.hmac_enabled"].value ? "Bekapcsolva" : "Kikapcsolva";
      }
      // Heartbeat
      if (cfg["heartbeat.interval"]) document.getElementById("cfgHbInterval").value = cfg["heartbeat.interval"].value;
      if (cfg["heartbeat.timeout"]) document.getElementById("cfgHbTimeout").value = cfg["heartbeat.timeout"].value;
      // Monitoring
      if (cfg["monitoring.dedup_cache_threshold"]) document.getElementById("cfgMonDedupThreshold").value = cfg["monitoring.dedup_cache_threshold"].value;
      if (cfg["monitoring.dedup_cleanup_interval"]) document.getElementById("cfgMonDedupCleanup").value = cfg["monitoring.dedup_cleanup_interval"].value;
    })
    .catch(function(e) { console.error("Shared config load error:", e); });
}

// ── Config Sync ──
function syncConfig() {
  var btn = event.target;
  btn.textContent = "⏳ Szinkronizálás...";
  btn.disabled = true;
  fetch("/api/config/sync", {
    method: "POST",
    headers: {"Authorization": "Bearer " + (localStorage.getItem("mesh_token") || "")}
  }).then(function(r) { return r.json(); })
    .then(function(data) {
      btn.textContent = "🔄 Szinkronizálás";
      btn.disabled = false;
      var result = document.getElementById("configSyncResult");
      var appliedCount = Object.keys(data.applied || {}).length;
      var skippedCount = Object.keys(data.skipped || {}).length;
      result.innerHTML = "<span style='color:var(--success)'>✅ " + appliedCount + " beállítás alkalmazva</span>" +
        (skippedCount > 0 ? ", <span style='color:var(--warning)'>" + skippedCount + " kihagyva</span>" : "");
      // Reload shared config to reflect changes
      loadSharedConfig();
    })
    .catch(function(e) {
      btn.textContent = "🔄 Szinkronizálás";
      btn.disabled = false;
      document.getElementById("configSyncResult").innerHTML = "<span style='color:var(--danger)'>❌ Hiba: " + e.message + "</span>";
    });
}

// ── Save All Settings (registry + health_scorer + shared config) ──
function saveAllSettings() {
  // 1. Validate health scorer weights sum to ~1.0
  var wLat = parseFloat(document.getElementById("settingsWeightLatency").value) || 0;
  var wSucc = parseFloat(document.getElementById("settingsWeightSuccess").value) || 0;
  var wAvail = parseFloat(document.getElementById("settingsWeightAvail").value) || 0;
  var wSum = wLat + wSucc + wAvail;
  if (Math.abs(wSum - 1.0) > 0.01) {
    alert("⚠️ Health Scorer súlyok összege " + wSum.toFixed(2) + " — kb. 1.0 kell legyen!\nLatencia: " + wLat + " + Siker: " + wSucc + " + Elérhetőség: " + wAvail + " = " + wSum.toFixed(2));
    return;
  }
  // 2. Validate intervals
  var hbInterval = parseInt(document.getElementById("cfgHbInterval").value);
  var hbTimeout = parseInt(document.getElementById("cfgHbTimeout").value);
  if (hbInterval >= hbTimeout) {
    alert("⚠️ Heartbeat interval (" + hbInterval + "s) < timeout (" + hbTimeout + "s) kell legyen!");
    return;
  }
  // 3. Save registry + health_scorer via /api/settings
  var regData = {
    registry: {
      auto_approve: document.getElementById("settingsAutoApprove").checked,
      health_check_interval: parseFloat(document.getElementById("settingsHealthInterval").value)
    },
    health_scorer: {
      weights: { latency: wLat, success_rate: wSucc, availability: wAvail },
      latency_threshold_ms: parseFloat(document.getElementById("settingsLatencyThreshold").value),
      decay_factor: parseFloat(document.getElementById("settingsDecayFactor").value),
      recovery_factor: parseFloat(document.getElementById("settingsRecoveryFactor").value)
    }
  };
  // 4. Save shared config via /api/config/shared
  var sharedData = {
    "delegation.expiry_minutes": parseInt(document.getElementById("cfgDelExpiry").value),
    "delegation.auto_renew": document.getElementById("cfgDelAutoRenew").checked,
    "delegation.cpu_threshold_p4": parseInt(document.getElementById("cfgDelCpuP4").value),
    "delegation.cpu_threshold_p7": parseInt(document.getElementById("cfgDelCpuP7").value),
    "security.rate_limit_per_minute": parseInt(document.getElementById("cfgSecRateLimit").value),
    "security.token_rotation_interval": parseInt(document.getElementById("cfgSecTokenRot").value),
    "security.mtls_enabled": document.getElementById("cfgSecMtls").checked,
    "security.hmac_enabled": document.getElementById("cfgSecHmac").checked,
    "heartbeat.interval": hbInterval,
    "heartbeat.timeout": hbTimeout,
    "monitoring.dedup_cache_threshold": parseInt(document.getElementById("cfgMonDedupThreshold").value),
    "monitoring.dedup_cleanup_interval": parseInt(document.getElementById("cfgMonDedupCleanup").value)
  };
  // Save both in parallel
  var tok = localStorage.getItem("mesh_token") || "";
  var p1 = fetch("/api/settings", {
    method: "POST",
    headers: {"Authorization": "Bearer " + tok, "Content-Type": "application/json"},
    body: JSON.stringify(regData)
  }).then(function(r) { return r.json(); });
  var p2 = fetch("/api/config/shared", {
    method: "POST",
    headers: {"Authorization": "Bearer " + tok, "Content-Type": "application/json"},
    body: JSON.stringify(sharedData)
  }).then(function(r) { return r.json(); });
  Promise.all([p1, p2])
    .then(function(results) {
      var ok1 = results[0].status === "ok";
      var ok2 = !results[1].error;
      if (ok1 && ok2) {
        alert("✅ Összes beállítás elmentve!\n\nRegistry + Health Scorer: ✅\nShared Config: ✅ (" + Object.keys(results[1].accepted || {}).length + " kulcs)\n\n⚠️ A shared config alkalmazásához kattints a \"Szinkronizálás\" gombra.");
        loadSettings();
      } else {
        alert("⚠️ Részleges mentés:\nRegistry: " + (ok1 ? "✅" : "❌ " + (results[0].error || "")) + "\nShared Config: " + (ok2 ? "✅" : "❌ " + (results[1].error || "")));
      }
    })
    .catch(function(e) {
      alert("❌ Mentési hiba: " + e.message);
    });
}

function loadPendingAgents() {
  fetch("/api/registry/pending", {headers: {"Authorization": "Bearer " + (localStorage.getItem("mesh_token") || "")}})
    .then(function(r) { return r.json(); })
    .then(function(data) {
      var container = document.getElementById("settingsPendingAgents");
      if (!data.pending || data.pending.length === 0) {
        container.innerHTML = "<div style='color:var(--text3)'>Nincs függő jóváhagyási kérelem ✅</div>";
        return;
      }
      var html = "<table style='width:100%;border-collapse:collapse;font-size:13px'>";
      html += "<tr style='border-bottom:1px solid var(--border)'><th style='text-align:left;padding:4px'>Név</th><th style='text-align:left;padding:4px'>Képességek</th><th style='text-align:left;padding:4px'>Endpoint</th><th style='text-align:right;padding:4px'>Művelet</th></tr>";
      data.pending.forEach(function(agent) {
        html += "<tr style='border-bottom:1px solid var(--border)'>";
        html += "<td style='padding:6px'><b>" + agent.name + "</b></td>";
        html += "<td style='padding:6px'>" + (agent.capabilities || []).join(", ") + "</td>";
        html += "<td style='padding:6px;font-size:11px'>" + (agent.endpoint || "-") + "</td>";
        html += "<td style='padding:6px;text-align:right'>";
        html += "<button class='btn btn-sm' style='background:var(--success);margin-right:4px' onclick='approveAgent(\"" + agent.name + "\")'>✅ Jóváhagy</button>";
        html += "<button class='btn btn-sm' style='background:var(--danger)' onclick='rejectAgent(\"" + agent.name + "\")'>❌ Elutasít</button>";
        html += "</td></tr>";
      });
      html += "</table>";
      container.innerHTML = html;
    })
    .catch(function(e) { console.error("Pending agents error:", e); });
}

function approveAgent(name) {
  fetch("/api/registry/approve/" + name, {
    method: "POST",
    headers: {"Authorization": "Bearer " + (localStorage.getItem("mesh_token") || "")}
  }).then(function(r) { return r.json(); })
    .then(function(data) {
      if (data.status === "approved") {
        alert("✅ " + name + " jóváhagyva!");
        loadPendingAgents();
      } else {
        alert("❌ Hiba: " + (data.error || "Ismeretlen hiba"));
      }
    });
}

function rejectAgent(name) {
  if (!confirm("Biztosan elutasítod " + name + " kérelmét?")) return;
  fetch("/api/registry/reject/" + name, {
    method: "POST",
    headers: {"Authorization": "Bearer " + (localStorage.getItem("mesh_token") || "")}
  }).then(function(r) { return r.json(); })
    .then(function(data) {
      if (data.status === "rejected") {
        alert("❌ " + name + " elutasítva.");
        loadPendingAgents();
      } else {
        alert("❌ Hiba: " + (data.error || "Ismeretlen hiba"));
      }
    });
}

function saveSettings() {
  var data = {
    registry: {
      auto_approve: document.getElementById("settingsAutoApprove").checked,
      health_check_interval: parseFloat(document.getElementById("settingsHealthInterval").value)
    },
    health_scorer: {
      weights: {
        latency: parseFloat(document.getElementById("settingsWeightLatency").value),
        success_rate: parseFloat(document.getElementById("settingsWeightSuccess").value),
        availability: parseFloat(document.getElementById("settingsWeightAvail").value)
      },
      latency_threshold_ms: parseFloat(document.getElementById("settingsLatencyThreshold").value),
      decay_factor: parseFloat(document.getElementById("settingsDecayFactor").value),
      recovery_factor: parseFloat(document.getElementById("settingsRecoveryFactor").value)
    }
  };
  fetch("/api/settings", {
    method: "POST",
    headers: {"Authorization": "Bearer " + (localStorage.getItem("mesh_token") || ""), "Content-Type": "application/json"},
    body: JSON.stringify(data)
  }).then(function(r) { return r.json(); })
    .then(function(result) {
      if (result.status === "ok") {
        alert("✅ Beállítások elmentve!");
        loadSettings();
      } else {
        alert("❌ Hiba: " + (result.error || "Ismeretlen hiba"));
      }
    });

  // Update auto-approve label
  document.getElementById("settingsAutoApproveLabel").textContent =
    document.getElementById("settingsAutoApprove").checked ? "Bekapcsolva" : "Kikapcsolva";
}

document.getElementById("settingsAutoApprove").addEventListener("change", function() {
  document.getElementById("settingsAutoApproveLabel").textContent = this.checked ? "Bekapcsolva" : "Kikapcsolva";
});

// Checkbox label updaters for new settings
document.getElementById("cfgDelAutoRenew").addEventListener("change", function() {
  document.getElementById("cfgDelAutoRenewLabel").textContent = this.checked ? "Bekapcsolva" : "Kikapcsolva";
});
document.getElementById("cfgSecMtls").addEventListener("change", function() {
  document.getElementById("cfgSecMtlsLabel").textContent = this.checked ? "Bekapcsolva" : "Kikapcsolva";
});
document.getElementById("cfgSecHmac").addEventListener("change", function() {
  document.getElementById("cfgSecHmacLabel").textContent = this.checked ? "Bekapcsolva" : "Kikapcsolva";
});

// ─── 1. Node Management Functions ───────────────────────────────
function loadSettingsNodes() {
  var tk = localStorage.getItem("mesh_token") || "";
  fetch("/api/nodes", {headers: {"Authorization": "Bearer " + tk}})
    .then(function(r) { return r.json(); })
    .then(function(data) {
      var nodes = data.nodes || [];
      var html = "<table style='width:100%;border-collapse:collapse;font-size:12px'>";
      html += "<tr style='border-bottom:1px solid var(--border)'><th style='text-align:left;padding:4px'>Node</th><th style='text-align:left;padding:4px'>Státusz</th><th style='text-align:left;padding:4px'>P2P</th><th style='text-align:left;padding:4px'>PG</th><th style='text-align:right;padding:4px'>Health</th><th style='text-align:left;padding:4px'>Skills</th></tr>";
      nodes.forEach(function(n) {
        var dot = n.status === "online" ? "🟢" : n.status === "connected" ? "🟡" : "🔴";
        var p2pIcon = n.p2p_available ? "✅" : "❌";
        var pgIcon = n.pg_available ? "✅" : "❌";
        var score = n.health_score !== undefined ? (n.health_score * 100).toFixed(0) + "%" : "-";
        var skills = (n.skills || []).slice(0, 3).join(", ");
        if ((n.skills || []).length > 3) skills += " +" + (n.skills.length - 3);
        html += "<tr style='border-bottom:1px solid var(--border)'>";
        html += "<td style='padding:4px'><b>" + escHtml(n.node_name || n.name || "?") + "</b><br><span style='font-size:10px;color:var(--text3)'>" + escHtml(n.host || "") + ":" + (n.p2p_port || "") + "</span></td>";
        html += "<td style='padding:4px'>" + dot + " " + escHtml(n.status || "?") + "</td>";
        html += "<td style='padding:4px;text-align:center'>" + p2pIcon + "</td>";
        html += "<td style='padding:4px;text-align:center'>" + pgIcon + "</td>";
        html += "<td style='padding:4px;text-align:right'>" + score + "</td>";
        html += "<td style='padding:4px;font-size:10px;color:var(--text3)'>" + escHtml(skills) + "</td>";
        html += "</tr>";
      });
      html += "</table>";
      document.getElementById("settingsNodeList").innerHTML = html;
    })
    .catch(function(e) {
      document.getElementById("settingsNodeList").innerHTML = "<span style='color:var(--danger)'>Hiba: " + escHtml(String(e)) + "</span>";
    });
  // Load pending nodes
  fetch("/api/nodes/pending", {headers: {"Authorization": "Bearer " + tk}})
    .then(function(r) { return r.json(); })
    .then(function(data) {
      var pn = data.nodes || [];
      if (pn.length === 0) {
        document.getElementById("settingsPendingNodes").innerHTML = "<span style='color:var(--text3)'>Nincs függő node ✅</span>";
        return;
      }
      var html = "<table style='width:100%;border-collapse:collapse;font-size:12px'>";
      pn.forEach(function(n) {
        html += "<tr><td style='padding:4px'><b>" + escHtml(n.node_name) + "</b></td>";
        html += "<td style='padding:4px;font-size:10px;color:var(--text3)'>" + escHtml(n.host || "") + "</td>";
        html += "<td style='padding:4px;text-align:right'>";
        html += "<button class='btn btn-sm' style='background:var(--success);margin-right:4px;font-size:10px' onclick='approveNode(\"" + n.node_name + "\")'>✅</button>";
        html += "<button class='btn btn-sm' style='background:var(--danger);font-size:10px' onclick='rejectNode(\"" + n.node_name + "\")'>❌</button>";
        html += "</td></tr>";
      });
      html += "</table>";
      document.getElementById("settingsPendingNodes").innerHTML = html;
    })
    .catch(function(e) {
      document.getElementById("settingsPendingNodes").innerHTML = "<span style='color:var(--text3)'>Nincs jogosultság vagy nincs függő node</span>";
    });
}

function approveNode(name) {
  fetch("/api/nodes/" + encodeURIComponent(name) + "/approve", {
    method: "POST",
    headers: {"Authorization": "Bearer " + (localStorage.getItem("mesh_token") || "")}
  }).then(function(r) { return r.json(); })
    .then(function(d) {
      if (d.status === "approved") { alert("✅ " + name + " elfogadva!"); loadSettingsNodes(); }
      else { alert("❌ Hiba: " + (d.error || "ismeretlen")); }
    });
}

function rejectNode(name) {
  if (!confirm("Biztosan elutasítod " + name + "-t?")) return;
  fetch("/api/nodes/" + encodeURIComponent(name) + "/reject", {
    method: "POST",
    headers: {"Authorization": "Bearer " + (localStorage.getItem("mesh_token") || "")}
  }).then(function(r) { return r.json(); })
    .then(function(d) {
      if (d.status === "rejected") { alert("❌ " + name + " elutasítva."); loadSettingsNodes(); }
      else { alert("❌ Hiba: " + (d.error || "ismeretlen")); }
    });
}

// ─── 2. Alert Rules Functions ───────────────────────────────────
function loadAlertRules() {
  var tk = localStorage.getItem("mesh_token") || "";
  fetch("/api/alerts", {headers: {"Authorization": "Bearer " + tk}})
    .then(function(r) { return r.json(); })
    .then(function(data) {
      var rules = data.rules || [];
      var activeAlerts = data.active_alerts || [];
      var html = "";
      if (activeAlerts.length > 0) {
        html += "<div style='margin-bottom:6px;padding:6px;background:var(--danger);border-radius:4px;color:#fff;font-size:11px'>";
        html += "🔴 <b>Aktív alertek:</b> " + activeAlerts.length + " db";
        activeAlerts.forEach(function(a) {
          html += "<br>" + escHtml(a.rule_name || a.id || "?") + ": " + escHtml(a.message || "");
        });
        html += "</div>";
      }
      if (rules.length === 0) {
        html += "<span style='color:var(--text3)'>Nincs alert szabály definiálva</span>";
      } else {
        html += "<table style='width:100%;border-collapse:collapse;font-size:11px'>";
        rules.forEach(function(r) {
          var sevIcon = r.severity === "critical" ? "🔴" : r.severity === "warning" ? "⚠️" : "ℹ️";
          var enabledBadge = r.enabled
            ? "<span style='color:var(--success)'>BE</span>"
            : "<span style='color:var(--text3)'>KI</span>";
          html += "<tr style='border-bottom:1px solid var(--border)'>";
          html += "<td style='padding:4px'>" + sevIcon + " <b>" + escHtml(r.name || r.id) + "</b><br>";
          html += "<span style='font-size:10px;color:var(--text3)'>" + escHtml(r.metric) + " " + escHtml(r.operator) + " " + r.threshold + " (cooldown: " + r.cooldown + "s)</span></td>";
          html += "<td style='padding:4px;text-align:right;white-space:nowrap'>";
          html += "<button class='btn btn-sm' style='font-size:10px;margin-right:4px' onclick='toggleAlertRule(\"" + r.id + "\")'>" + enabledBadge + "</button>";
          html += "<button class='btn btn-sm' style='background:var(--danger);color:#fff;font-size:10px' onclick='deleteAlertRule(\"" + r.id + "\")'>🗑️</button>";
          html += "</td></tr>";
        });
        html += "</table>";
      }
      document.getElementById("settingsAlertRules").innerHTML = html;
    })
    .catch(function(e) {
      document.getElementById("settingsAlertRules").innerHTML = "<span style='color:var(--danger)'>Hiba: " + escHtml(String(e)) + "</span>";
    });
}

function addAlertRule() {
  var ruleId = document.getElementById("alertRuleId").value.trim();
  if (!ruleId) { alert("❌ Azonosító kötelező!"); return; }
  var data = {
    id: ruleId,
    name: document.getElementById("alertRuleName").value.trim() || ruleId,
    metric: document.getElementById("alertRuleMetric").value,
    operator: document.getElementById("alertRuleOperator").value,
    threshold: parseFloat(document.getElementById("alertRuleThreshold").value),
    severity: document.getElementById("alertRuleSeverity").value,
    cooldown: parseFloat(document.getElementById("alertRuleCooldown").value) || 300,
    enabled: true
  };
  if (isNaN(data.threshold)) { alert("❌ Érvénytelen küszöbérték!"); return; }
  fetch("/api/alerts/rules", {
    method: "POST",
    headers: {"Authorization": "Bearer " + (localStorage.getItem("mesh_token") || ""), "Content-Type": "application/json"},
    body: JSON.stringify(data)
  }).then(function(r) { return r.json(); })
    .then(function(d) {
      if (d.status === "ok") {
        alert("✅ Alert szabály hozzáadva!");
        document.getElementById("alertRuleForm").style.display = "none";
        document.getElementById("alertRuleId").value = "";
        document.getElementById("alertRuleName").value = "";
        document.getElementById("alertRuleThreshold").value = "";
        loadAlertRules();
      } else { alert("❌ Hiba: " + (d.error || "ismeretlen")); }
    });
}

function toggleAlertRule(ruleId) {
  fetch("/api/alerts/rules/" + encodeURIComponent(ruleId) + "/toggle", {
    method: "POST",
    headers: {"Authorization": "Bearer " + (localStorage.getItem("mesh_token") || "")}
  }).then(function(r) { return r.json(); })
    .then(function(d) { if (d.status === "ok") loadAlertRules(); });
}

function deleteAlertRule(ruleId) {
  if (!confirm("Biztosan törlöd az alert szabályt?")) return;
  fetch("/api/alerts/rules/" + encodeURIComponent(ruleId), {
    method: "DELETE",
    headers: {"Authorization": "Bearer " + (localStorage.getItem("mesh_token") || "")}
  }).then(function(r) { return r.json(); })
    .then(function(d) { if (d.status === "deleted") loadAlertRules(); });
}

// ─── 3. P2P Connection Tools ────────────────────────────────────
function p2pResetBackoff() {
  fetch("/api/p2p/reset-backoff", {
    method: "POST",
    headers: {"Authorization": "Bearer " + (localStorage.getItem("mesh_token") || ""), "Content-Type": "application/json"},
    body: JSON.stringify({})
  }).then(function(r) { return r.json(); })
    .then(function(d) {
      if (d.status === "ok") {
        document.getElementById("p2pActionResult").innerHTML =
          "<span style='color:var(--success)'>✅ " + (d.backoffs_reset || 0) + " backoff törölve</span>";
      } else {
        document.getElementById("p2pActionResult").innerHTML = "<span style='color:var(--danger)'>❌ " + escHtml(d.error || "") + "</span>";
      }
    });
}

function p2pReconnect() {
  document.getElementById("p2pActionResult").innerHTML = "<span style='color:var(--info)'>📡 Újracsatlakozás folyamatban...</span>";
  fetch("/api/p2p/reconnect", {
    method: "POST",
    headers: {"Authorization": "Bearer " + (localStorage.getItem("mesh_token") || ""), "Content-Type": "application/json"},
    body: JSON.stringify({})
  }).then(function(r) { return r.json(); })
    .then(function(d) {
      if (d.status === "ok") {
        document.getElementById("p2pActionResult").innerHTML =
          "<span style='color:var(--success)'>✅ Újracsatlakozás indítva: " + escHtml(d.discovery_result || "OK") + "</span>";
      } else {
        document.getElementById("p2pActionResult").innerHTML = "<span style='color:var(--danger)'>❌ " + escHtml(d.error || "") + "</span>";
      }
    });
}

// ─── 4. Task Cleanup ────────────────────────────────────────────
function taskCleanup() {
  var maxAge = parseInt(document.getElementById("cleanupMaxAge").value) || 24;
  if (maxAge < 1) maxAge = 1;
  if (maxAge > 720) maxAge = 720;
  if (!confirm("Biztosan törlöd az összes befejezett/vissszavont delegációt ami régebbi mint " + maxAge + " óra?")) return;
  document.getElementById("cleanupResult").innerHTML = "<span style='color:var(--info)'>🗑️ Tisztítás folyamatban...</span>";
  fetch("/api/tasks/cleanup?max_age_hours=" + maxAge, {
    method: "POST",
    headers: {"Authorization": "Bearer " + (localStorage.getItem("mesh_token") || "")}
  }).then(function(r) { return r.json(); })
    .then(function(d) {
      if (d.deleted !== undefined) {
        document.getElementById("cleanupResult").innerHTML =
          "<span style='color:var(--success)'>✅ " + d.deleted + " delegáció törölve (régebbi mint " + d.max_age_hours + " óra)</span>";
      } else {
        document.getElementById("cleanupResult").innerHTML = "<span style='color:var(--danger)'>❌ " + escHtml(d.error || "") + "</span>";
      }
    });
}

// ─── 5. Memory Sync ─────────────────────────────────────────────
function memorySync() {
  document.getElementById("memorySyncResult").innerHTML = "<span style='color:var(--info)'>🧠 Szinkronizálás folyamatban...</span>";
  fetch("/api/memory/sync", {
    method: "POST",
    headers: {"Authorization": "Bearer " + (localStorage.getItem("mesh_token") || ""), "Content-Type": "application/json"},
    body: JSON.stringify({})
  }).then(function(r) { return r.json(); })
    .then(function(d) {
      if (d.synced !== undefined) {
        document.getElementById("memorySyncResult").innerHTML =
          "<span style='color:var(--success)'>✅ " + d.synced + " memória bejegyzés szinkronizálva</span>";
      } else {
        document.getElementById("memorySyncResult").innerHTML = "<span style='color:var(--danger)'>❌ " + escHtml(d.error || "") + "</span>";
      }
    });
}

// ─── User Management Functions ──────────────────────────────────
var _changePasswordUsername = "";

function loadUsers() {
  // Only show for owners
  if (!authUser || authUser.role !== "owner") {
    document.getElementById("usersSection").style.display = "none";
    return;
  }
  document.getElementById("usersSection").style.display = "block";
  fetch("/api/auth/users", {headers: {"Authorization": "Bearer " + (localStorage.getItem("mesh_token") || "")}})
    .then(function(r) { return r.json(); })
    .then(function(d) {
      var tbody = document.getElementById("usersTableBody");
      if (d.error) {
        tbody.innerHTML = '<tr><td colspan="5" style="text-align:center;color:var(--danger)">' + d.error + '</td></tr>';
        return;
      }
      if (!d.users || d.users.length === 0) {
        tbody.innerHTML = '<tr><td colspan="5" style="text-align:center;color:var(--text3)">Nincs felhasználó</td></tr>';
        return;
      }
      tbody.innerHTML = d.users.map(function(u) {
        var isSelf = u.username === authUser.username;
        var roleBadge = u.role === "owner"
          ? '<span class="role-badge owner">tulajdonos</span>'
          : '<span class="role-badge user">felhasználó</span>';
        var lastLogin = u.last_login ? new Date(u.last_login * 1000).toLocaleString("hu-HU") : "Soha";
        var actions = "";
        if (!isSelf) {
          actions += '<button class="btn btn-sm" style="background:var(--warning);margin-right:4px" onclick="showChangePassword(\'' + escapeHtml(u.username) + '\')">🔑</button>';
          actions += '<button class="btn btn-sm" style="background:var(--danger)" onclick="deleteUser(\'' + escapeHtml(u.username) + '\')">🗑️</button>';
        } else {
          actions = '<span style="font-size:11px;color:var(--text3)">Jelenlegi</span>';
        }
        return '<tr style="border-bottom:1px solid var(--border)">' +
          '<td style="padding:8px"><b>' + escapeHtml(u.username) + '</b></td>' +
          '<td style="padding:8px">' + escapeHtml(u.display_name || u.username) + '</td>' +
          '<td style="padding:8px">' + roleBadge + '</td>' +
          '<td style="padding:8px;font-size:12px;color:var(--text3)">' + lastLogin + '</td>' +
          '<td style="padding:8px;text-align:right">' + actions + '</td>' +
        '</tr>';
      }).join('');
    })
    .catch(function(e) {
      document.getElementById("usersTableBody").innerHTML = '<tr><td colspan="5" style="text-align:center;color:var(--danger)">Hiba a felhasználók betöltésekor</td></tr>';
    });
}

function addUser() {
  var username = document.getElementById("newUserUsername").value.trim().toLowerCase();
  var displayName = document.getElementById("newUserDisplayName").value.trim() || username;
  var password = document.getElementById("newUserPassword").value;
  var role = document.getElementById("newUserRole").value;
  var errEl = document.getElementById("addUserError");
  errEl.textContent = "";

  if (!username || !password) {
    errEl.textContent = "Felhasználónév és jelszó kötelező!";
    return;
  }
  if (password.length < 6) {
    errEl.textContent = "A jelszó legalább 6 karakter!";
    return;
  }

  fetch("/api/auth/register", {
    method: "POST",
    headers: {"Content-Type": "application/json", "Authorization": "Bearer " + (localStorage.getItem("mesh_token") || "")},
    body: JSON.stringify({username: username, display_name: displayName, password: password, role: role})
  }).then(function(r) { return r.json(); }).then(function(d) {
    if (d.error) {
      errEl.textContent = d.error;
    } else {
      errEl.style.color = "var(--success)";
      errEl.textContent = "✅ Felhasználó '" + d.user.username + "' létrehozva!";
      document.getElementById("newUserUsername").value = "";
      document.getElementById("newUserDisplayName").value = "";
      document.getElementById("newUserPassword").value = "";
      loadUsers();
      setTimeout(function() { errEl.style.color = ""; errEl.textContent = ""; }, 3000);
    }
  }).catch(function() { errEl.textContent = "Hálózati hiba"; });
}

function deleteUser(username) {
  if (!confirm("Biztosan törlöd '" + username + "' felhasználót? Ez a fiók deaktiválódik.")) return;
  fetch("/api/auth/users/" + encodeURIComponent(username), {
    method: "DELETE",
    headers: {"Authorization": "Bearer " + (localStorage.getItem("mesh_token") || "")}
  }).then(function(r) { return r.json(); }).then(function(d) {
    if (d.status === "deleted") {
      alert("✅ '" + username + "' felhasználó törölve!");
      loadUsers();
    } else {
      alert("❌ Hiba: " + (d.error || "Ismeretlen hiba"));
    }
  }).catch(function() { alert("Hálózati hiba"); });
}

function showChangePassword(username) {
  _changePasswordUsername = username;
  document.getElementById("changePasswordUser").textContent = username;
  document.getElementById("changePasswordForm").style.display = "block";
  document.getElementById("changePasswordNewPw").value = "";
  document.getElementById("changePasswordError").textContent = "";
  document.getElementById("changePasswordNewPw").focus();
}

function cancelChangePassword() {
  _changePasswordUsername = "";
  document.getElementById("changePasswordForm").style.display = "none";
}

function submitChangePassword() {
  var newPassword = document.getElementById("changePasswordNewPw").value;
  var errEl = document.getElementById("changePasswordError");
  errEl.textContent = "";

  if (!newPassword || newPassword.length < 6) {
    errEl.textContent = "Az új jelszó legalább 6 karakter!";
    return;
  }

  fetch("/api/auth/users/" + encodeURIComponent(_changePasswordUsername) + "/password", {
    method: "PUT",
    headers: {"Content-Type": "application/json", "Authorization": "Bearer " + (localStorage.getItem("mesh_token") || "")},
    body: JSON.stringify({new_password: newPassword})
  }).then(function(r) { return r.json(); }).then(function(d) {
    if (d.status === "password_changed") {
      alert("✅ '" + _changePasswordUsername + "' jelszava sikeresen módosítva!");
      cancelChangePassword();
    } else {
      errEl.textContent = d.error || "Ismeretlen hiba";
    }
  }).catch(function() { errEl.textContent = "Hálózati hiba"; });
}

// ── Image Generation (Pollinations.ai) ───────────────────────
function showImageGen() {
  document.getElementById('imageGenModal').style.display = 'flex';
  document.getElementById('imageGenResult').style.display = 'none';
  document.getElementById('imagePrompt').focus();
}

function generateImage() {
  var prompt = document.getElementById('imagePrompt').value.trim();
  if (!prompt) { alert('Adj meg egy leírást!'); return; }
  var width = parseInt(document.getElementById('imageWidth').value) || 512;
  var height = parseInt(document.getElementById('imageHeight').value) || 512;
  var model = document.getElementById('imageModel').value;
  var seedVal = document.getElementById('imageSeed').value;
  var seed = seedVal ? parseInt(seedVal) : undefined;

  var resultDiv = document.getElementById('imageGenResult');
  var statusEl = document.getElementById('imageGenStatus');
  var previewEl = document.getElementById('imageGenPreview');
  var btn = document.getElementById('imageGenBtn');

  resultDiv.style.display = 'block';
  statusEl.textContent = '⏳ Kép generálása...';
  previewEl.style.display = 'none';
  btn.disabled = true;
  btn.textContent = '⏳ Generálás...';

  var body = {prompt: prompt, width: width, height: height, model: model};
  if (seed !== undefined) body.seed = seed;

  fetch('/api/image/generate', {
    method: 'POST',
    headers: {'Content-Type': 'application/json', 'Authorization': 'Bearer ' + authToken},
    body: JSON.stringify(body)
  }).then(function(r) { return r.json(); }).then(function(d) {
    if (d.error) {
      statusEl.textContent = '❌ Hiba: ' + d.error;
      btn.disabled = false;
      btn.textContent = '🎨 Generálás';
      return;
    }
    statusEl.textContent = '⏳ Kép letöltése... (seed: ' + d.seed + ')';

    // Load image through proxy first, fallback to direct URL
    var proxyUrl = '/api/image/proxy?url=' + encodeURIComponent(d.url);
    previewEl.style.display = 'none';
    previewEl.onload = function() {
      previewEl.style.display = 'block';
      statusEl.textContent = '✅ Kép kész! (seed: ' + d.seed + ', modell: ' + d.model + ')';
      btn.disabled = false;
      btn.textContent = '🎨 Generálás';
    };
    previewEl.onerror = function() {
      // If proxy fails, try direct Pollinations URL
      if (previewEl.src.includes('/api/image/proxy')) {
        previewEl.src = d.url;
        statusEl.textContent = '⏳ Direct letöltés... (seed: ' + d.seed + ')';
      } else {
        previewEl.style.display = 'none';
        statusEl.innerHTML = '❌ Kép letöltés sikertelen. <a href="' + d.url + '" target="_blank" style="color:var(--primary)">Kattints ide a megtekintéshez</a>';
        btn.disabled = false;
        btn.textContent = '🎨 Generálás';
      }
    };
    previewEl.src = proxyUrl;
  }).catch(function(e) {
    statusEl.textContent = '❌ Hiba: ' + e.message;
    btn.disabled = false;
    btn.textContent = '🎨 Generálás';
  });
}

// ─── Skills Marketplace Modal ──────────────────────────────
function _skillsApiCall(url, method, body, _retry) {
  _retry = _retry || 0;
  var token = localStorage.getItem('mesh_token') || localStorage.getItem('a2a_token') || authToken || '';
  var opts = { method: method || 'GET', headers: { 'Authorization': 'Bearer ' + token } };
  if (body) { opts.headers['Content-Type'] = 'application/json'; opts.body = JSON.stringify(body); }
  return fetch(url, opts).then(function(r) {
    if (r.status === 429 && _retry < 3) {
      // Auto-retry on rate limit with exponential backoff
      return new Promise(function(resolve) { setTimeout(resolve, 1000 * (_retry + 1)); })
        .then(function() { return _skillsApiCall(url, method, body, _retry + 1); });
    }
    if (!r.ok) return r.json().then(function(e) { throw e.error || 'API error'; });
    return r.json();
  });
}

function showSkills() {
  var m = document.getElementById('skillsModal');
  m.style.display = 'flex';
  loadSkills();
  loadSkillStats();
}

function loadSkillStats() {
  _skillsApiCall('/api/skills/stats').then(function(d) {
    var bar = document.getElementById('skillStatsBar');
    if (!bar) return;
    var html = '';
    html += '<div style="background:var(--surface2);border:1px solid var(--border);border-radius:8px;padding:8px 16px;text-align:center;min-width:100px">';
    html += '<div style="font-size:24px;font-weight:700;color:var(--primary)">' + (d.total_skills || 0) + '</div>';
    html += '<div style="font-size:11px;color:var(--text3)">Összes Skill</div></div>';
    html += '<div style="background:var(--surface2);border:1px solid var(--border);border-radius:8px;padding:8px 16px;text-align:center;min-width:100px">';
    html += '<div style="font-size:24px;font-weight:700;color:var(--purple)">' + (d.auto_generated || 0) + '</div>';
    html += '<div style="font-size:11px;color:var(--text3)">Auto-generált</div></div>';
    html += '<div style="background:var(--surface2);border:1px solid var(--border);border-radius:8px;padding:8px 16px;text-align:center;min-width:100px">';
    html += '<div style="font-size:24px;font-weight:700;color:var(--green)">' + (d.published_files || 0) + '</div>';
    html += '<div style="font-size:11px;color:var(--text3)">Publikált fájlok</div></div>';
    // Agent badges
    var byAgent = d.by_agent || {};
    Object.keys(byAgent).forEach(function(name) {
      var colors = {nova:'#58a6ff', morzsa:'#bc8cff', runa:'#3fb950', tor:'#d29922', lifecycle_node:'#f85149'};
      var color = colors[name] || '#8b949e';
      html += '<div style="background:var(--surface2);border:1px solid var(--border);border-radius:20px;padding:6px 12px;font-size:12px;display:flex;align-items:center;gap:4px">';
      html += '<span style="width:8px;height:8px;border-radius:50%;background:' + color + '"></span>';
      html += escHtml(name) + ' <span style="font-weight:700;color:var(--primary)">' + byAgent[name] + '</span></div>';
    });
    bar.innerHTML = html;
  }).catch(function(e) { /* silent */ });
}

function syncSkillsModal() {
  _skillsApiCall('/api/skills/auto-sync', 'POST', {}).then(function(d) {
    alert('✅ ' + (d.synced || 0) + ' skill szinkronizálva!');
    loadSkills();
    loadSkillStats();
  }).catch(function(e) { alert('Szinkron hiba: ' + e); });
}

function showOnboardPanel() {
  document.getElementById('onboardPanel').style.display = '';
  document.getElementById('advertiseSkillForm').style.display = 'none';
}

function runOnboard() {
  var nodeName = document.getElementById('onboardName').value.trim();
  var sshKey = document.getElementById('onboardKey').value.trim();
  var useTS = document.getElementById('onboardTailscale').checked;
  var role = document.getElementById('onboardRole') ? document.getElementById('onboardRole').value : 'auto';
  var autoApprove = document.getElementById('onboardAutoApprove') ? document.getElementById('onboardAutoApprove').checked : false;
  if (!nodeName) { alert('Node név kötelező!'); return; }
  
  var resultDiv = document.getElementById('onboardResult');
  resultDiv.innerHTML = '<div style="text-align:center;padding:12px;color:var(--text3)">⏳ Onboarding folyamatban...</div>';
  
  _skillsApiCall('/api/onboard', 'POST', { node_name: nodeName, ssh_pubkey: sshKey, use_tailscale: useTS, role: role, auto_approve: autoApprove }).then(function(d) {
    if (d.error) {
      resultDiv.innerHTML = '<div style="color:var(--red);padding:8px">❌ ' + escHtml(d.error) + '</div>';
      return;
    }
    var html = '<div style="padding:8px;font-size:12px">';
    html += '<div style="font-weight:600;margin-bottom:6px">' + (d.success ? '✅ Sikeres!' : '⚠️ Befejezve') + '</div>';
    if (d.steps) d.steps.forEach(function(s) {
      var icon = s.status === 'ok' ? '✅' : s.status === 'warn' ? '⚠️' : '❌';
      html += '<div>' + icon + ' ' + escHtml(s.step) + (s.detail ? ' — ' + escHtml(s.detail) : '') + '</div>';
    });
    if (d.mesh_user) {
      html += '<div style="margin-top:6px;padding:6px;background:var(--bg);border-radius:4px">';
      html += '🔐 User: <code>' + escHtml(d.mesh_user.username) + '</code> Pass: <code>' + escHtml(d.mesh_user.password) + '</code></div>';
    }
    if (d.peer_info) {
      html += '<div style="margin-top:4px">📡 Peers: ' + d.peer_info.map(function(p){return p.name+'@'+p.host;}).join(', ') + '</div>';
    }
    if (d.tailscale_ips) {
      html += '<div style="margin-top:4px">🔵 Tailscale: ' + Object.keys(d.tailscale_ips).map(function(n){return n+'='+d.tailscale_ips[n];}).join(', ') + '</div>';
    }
    html += '</div>';
    resultDiv.innerHTML = html;
  }).catch(function(e) {
    resultDiv.innerHTML = '<div style="color:var(--red);padding:8px">❌ ' + escHtml(e) + '</div>';
  });
}

function scanOnboardNodes() {
  var statusDiv = document.getElementById('onboardDiscoveryText');
  var listDiv = document.getElementById('onboardPendingList');
  if (statusDiv) statusDiv.textContent = '🔍 Keresés folyamatban...';
  if (listDiv) listDiv.innerHTML = '';
  
  _skillsApiCall('/api/onboard/scan', 'POST', {}).then(function(d) {
    if (d.error) {
      if (statusDiv) statusDiv.textContent = '❌ Hiba: ' + d.error;
      return;
    }
    var found = d.discovered || [];
    if (statusDiv) statusDiv.textContent = '🔍 ' + found.length + ' új node találva';
    
    if (found.length === 0) {
      if (listDiv) listDiv.innerHTML = '<div style="font-size:12px;color:var(--text3);padding:8px">Nem található új node. Keresd a Tailscale hálózaton vagy VPN-en.</div>';
      return;
    }
    
    var html = '';
    found.forEach(function(n) {
      var roleIcon = n.role === 'coordinator' ? '👑' : n.role === 'router' ? '📡' : '🔋';
      html += '<div style="display:flex;align-items:center;gap:8px;padding:8px;background:var(--bg);border-radius:6px;margin-bottom:4px">';
      html += '<span style="font-size:16px">' + roleIcon + '</span>';
      html += '<div style="flex:1">';
      html += '<div style="font-weight:600;font-size:13px">' + escHtml(n.name) + '</div>';
      html += '<div style="font-size:11px;color:var(--text3)">' + escHtml(n.host) + ' · ' + escHtml(n.platform || 'unknown') + '</div>';
      html += '</div>';
      html += '<button class="btn btn-sm" style="background:var(--success)" onclick="approveOnboardNode(\'' + escHtml(n.name) + '\',\'' + escHtml(n.host) + '\')">✅ Jóváhagy</button>';
      html += '<button class="btn btn-sm" style="background:var(--red);color:#fff" onclick="rejectOnboardNode(\'' + escHtml(n.name) + '\')">❌ Elutasít</button>';
      html += '</div>';
    });
    if (listDiv) listDiv.innerHTML = html;
  }).catch(function(e) {
    if (statusDiv) statusDiv.textContent = '❌ Hiba: ' + e;
  });
}

function approveOnboardNode(name, host) {
  var sshKey = document.getElementById('onboardKey') ? document.getElementById('onboardKey').value.trim() : '';
  var role = document.getElementById('onboardRole') ? document.getElementById('onboardRole').value : 'auto';
  var useTS = document.getElementById('onboardTailscale') ? document.getElementById('onboardTailscale').checked : true;
  
  if (!confirm('Jóváhagyod a(z) "' + name + '" node-ot? (host: ' + host + ')')) return;
  
  var resultDiv = document.getElementById('onboardResult');
  resultDiv.innerHTML = '<div style="text-align:center;padding:12px;color:var(--text3)">⏳ Onboarding: ' + name + '...</div>';
  
  _skillsApiCall('/api/onboard', 'POST', { node_name: name, ssh_pubkey: sshKey, use_tailscale: useTS, role: role, auto_approve: false }).then(function(d) {
    if (d.error) {
      resultDiv.innerHTML = '<div style="color:var(--red);padding:8px">❌ ' + escHtml(d.error) + '</div>';
      return;
    }
    var html = '<div style="padding:8px;font-size:12px">';
    html += '<div style="font-weight:600;margin-bottom:6px">' + (d.success ? '✅ ' + name + ' sikeresen onboardolva!' : '⚠️ Befejezve') + '</div>';
    if (d.steps) d.steps.forEach(function(s) {
      var icon = s.status === 'ok' ? '✅' : s.status === 'warn' ? '⚠️' : '❌';
      html += '<div>' + icon + ' ' + escHtml(s.step) + (s.detail ? ' — ' + escHtml(s.detail) : '') + '</div>';
    });
    if (d.mesh_user) {
      html += '<div style="margin-top:6px;padding:6px;background:var(--bg);border-radius:4px">';
      html += '🔐 User: <code>' + escHtml(d.mesh_user.username) + '</code> Pass: <code>' + escHtml(d.mesh_user.password) + '</code></div>';
    }
    html += '</div>';
    resultDiv.innerHTML = html;
    scanOnboardNodes(); // Refresh list
  }).catch(function(e) {
    resultDiv.innerHTML = '<div style="color:var(--red);padding:8px">❌ ' + escHtml(e) + '</div>';
  });
}

function rejectOnboardNode(name) {
  if (!confirm('Elutasítod a(z) "' + name + '" node-ot?')) return;
  _skillsApiCall('/api/onboard/reject', 'POST', { node_name: name }).then(function(d) {
    scanOnboardNodes(); // Refresh list
  }).catch(function(e) { /* silent */ });
}

function loadSkills() {
  _skillsApiCall('/api/skills').then(function(d) {
    renderSkills(d.skills || []);
  }).catch(function(e) { alert('Hiba: ' + e); });
}

function searchSkills() {
  var q = document.getElementById('skillSearchInput').value.trim();
  if (!q) { loadSkills(); return; }
  _skillsApiCall('/api/skills/search?q=' + encodeURIComponent(q)).then(function(d) {
    renderSkills(d.results || []);
  }).catch(function(e) { alert('Hiba: ' + e); });
}

function renderSkills(skills) {
  var c = document.getElementById('skillsList');
  if (!skills.length) {
    c.innerHTML = '<div style="text-align:center;padding:24px;color:var(--text3)">Nincsenek skill-ek</div>';
    return;
  }
  c.innerHTML = skills.map(function(s) {
    var tags = (s.tags || []).map(function(t) { return '<span style="background:var(--surface2);padding:2px 8px;border-radius:10px;font-size:11px;margin:2px">' + escHtml(t) + '</span>'; }).join('');
    var successRate = s.success_rate !== null && s.success_rate !== undefined ? (s.success_rate * 100).toFixed(0) + '%' : '—';
    var latency = s.avg_latency_ms !== null ? s.avg_latency_ms.toFixed(0) + 'ms' : '—';
    var stars = '';
    var avgRating = (s.metadata && s.metadata.avg_rating) ? s.metadata.avg_rating : 0;
    if (avgRating) {
      for (var i = 1; i <= 5; i++) { stars += i <= Math.round(avgRating) ? '★' : '☆'; }
    }
    return '<div style="background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:12px;margin-bottom:8px">' +
      '<div style="display:flex;justify-content:space-between;align-items:start">' +
        '<div style="flex:1">' +
          '<div style="font-weight:600;font-size:14px">' + escHtml(s.display_name || s.skill_name) + (stars ? ' <span style="color:var(--yellow);font-size:12px">' + stars + '</span>' : '') + '</div>' +
          '<div style="font-size:12px;color:var(--text3);margin:2px 0">agent: ' + escHtml(s.agent) + ' · latency: ' + latency + ' · success: ' + successRate + '</div>' +
          (s.description ? '<div style="font-size:13px;color:var(--text2);margin:4px 0">' + escHtml(s.description) + '</div>' : '') +
          (tags ? '<div style="margin-top:4px">' + tags + '</div>' : '') +
        '</div>' +
        '<div style="display:flex;flex-direction:column;gap:4px;margin-left:8px">' +
          '<button class="btn btn-sm" style="background:var(--primary);color:#fff" onclick="delegateToSkill(\'' + s.skill_id + '\')">Delegate</button>' +
          '<button class="btn btn-sm" style="background:var(--yellow);color:#000;font-size:11px" onclick="rateSkill(\'' + s.skill_id + '\')">Rate</button>' +
          '<button class="btn btn-sm" style="background:var(--red);color:#fff;font-size:11px" onclick="deleteSkill(\'' + s.skill_id + '\')">Delete</button>' +
        '</div>' +
      '</div>' +
    '</div>';
  }).join('');
}

function showAdvertiseSkillForm() {
  document.getElementById('advertiseSkillForm').style.display = '';
}

function advertiseSkill() {
  var name = document.getElementById('advSkillName').value.trim();
  var display = document.getElementById('advSkillDisplay').value.trim();
  var desc = document.getElementById('advSkillDesc').value.trim();
  var tagsStr = document.getElementById('advSkillTags').value.trim();
  var tags = tagsStr ? tagsStr.split(',').map(function(t) { return t.trim(); }).filter(Boolean) : [];
  if (!name) { alert('skill_name kotelezo'); return; }
  _skillsApiCall('/api/skills/advertise', 'POST', {
    skill_name: name,
    display_name: display || name,
    description: desc,
    tags: tags,
    cost: 0.0,
    max_concurrent: 3
  }).then(function(d) {
    alert('Skill hirdetve!');
    document.getElementById('advertiseSkillForm').style.display = 'none';
    document.getElementById('advSkillName').value = '';
    document.getElementById('advSkillDisplay').value = '';
    document.getElementById('advSkillDesc').value = '';
    document.getElementById('advSkillTags').value = '';
    loadSkills();
  }).catch(function(e) { alert('Hiba: ' + e); });
}

function delegateToSkill(skillId) {
  var task = prompt('Add meg a feladatot a skill szamara:');
  if (!task) return;
  _skillsApiCall('/api/skills/' + skillId + '/delegate', 'POST', { task: task }).then(function(d) {
    alert('Delegalva! task_id: ' + (d.task_id || d.delegation_id || '?'));
  }).catch(function(e) { alert('Hiba: ' + e); });
}

function rateSkill(skillId) {
  var rating = prompt('Ertekeles (1-5):');
  if (!rating) return;
  var r = parseInt(rating);
  if (r < 1 || r > 5) { alert('1-5 kozott'); return; }
  var feedback = prompt('Visszajelzes (opcionalis):') || '';
  _skillsApiCall('/api/skills/' + skillId + '/rate', 'POST', { rating: r, feedback: feedback }).then(function(d) {
    alert('Ertekelve! Atlag: ' + (d.avg_rating || '?') + ' (' + d.total_ratings + ' ertekeles)');
    loadSkills();
  }).catch(function(e) { alert('Hiba: ' + e); });
}

function deleteSkill(skillId) {
  if (!confirm('Biztosan torlod ezt a skill-t?')) return;
  _skillsApiCall('/api/skills/' + skillId, 'DELETE').then(function(d) {
    alert('Skill torolve');
    loadSkills();
  }).catch(function(e) { alert('Hiba: ' + e); });
}

// ─── Workflow Modal ────────────────────────────────────────
function showWorkflows() {
  var m = document.getElementById('workflowModal');
  m.style.display = 'flex';
  loadWorkflows();
}

function loadWorkflows() {
  _skillsApiCall('/api/workflows').then(function(d) {
    var c = document.getElementById('workflowList');
    var wfs = d.workflows || [];
    if (!wfs.length) {
      c.innerHTML = '<div style="text-align:center;padding:24px;color:var(--text3)">Nincsenek workflow-k</div>';
      return;
    }
    c.innerHTML = wfs.map(function(w) {
      var status = w.status || '?';
      var statusColors = {completed:'var(--green)', running:'var(--primary)', failed:'var(--red)', timeout:'var(--orange)', skipped:'var(--text3)', retrying:'var(--yellow)', pending:'var(--text3)'};
      var statusIcons = {completed:'✅', running:'🔄', failed:'❌', timeout:'⏱️', skipped:'⏭️', retrying:'🔁', pending:'⏳'};
      var color = statusColors[status] || 'var(--text3)';
      var icon = statusIcons[status] || '❓';
      var tasks = w.results || {};
      var taskKeys = Object.keys(tasks);
      var completedTasks = taskKeys.filter(function(t) { return tasks[t].status === 'completed'; }).length;
      var totalTasks = w.tasks || taskKeys.length || 0;
      var progress = totalTasks ? Math.round(completedTasks / totalTasks * 100) : 0;
      var active = w.active;
      var time = w.created_at ? new Date(w.created_at * 1000).toLocaleString('hu-HU', {month:'short',day:'numeric',hour:'2-digit',minute:'2-digit'}) : '';
      var html = '<div style="background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:12px;margin-bottom:8px' + (active ? ';border-left:3px solid var(--primary)' : '') + '">';
      html += '<div style="display:flex;justify-content:space-between;align-items:center">';
      html += '<div><span style="font-weight:600">' + icon + ' ' + escHtml(w.name || w.id) + '</span>';
      html += ' <span style="font-size:11px;padding:2px 8px;border-radius:10px;background:' + color + ';color:#fff">' + status + '</span>';
      if (active) html += ' <span style="font-size:10px;color:var(--primary)">● aktív</span>';
      html += '</div>';
      html += '<div style="display:flex;gap:4px">';
      html += '<button class="btn btn-sm" style="background:var(--surface2);color:var(--text)" onclick="checkWorkflowStatus(\'' + w.id + '\')">📊 Részletek</button>';
      if (!active) html += '<button class="btn btn-sm" style="background:var(--danger);color:#fff" onclick="deleteWorkflow(\'' + w.id + '\')">🗑️ Törlés</button>';
      html += '</div></div>';
      // Progress bar
      html += '<div style="margin-top:6px"><div style="font-size:11px;color:var(--text3);margin-bottom:3px">' + completedTasks + '/' + totalTasks + ' task · ' + (w.consensus || '?') + ' · ' + time + '</div>';
      html += '<div style="background:var(--surface2);border-radius:4px;height:6px;overflow:hidden"><div style="background:' + color + ';height:100%;width:' + progress + '%;transition:width .3s"></div></div></div>';
      // Task results (for completed workflows)
      if (!active && taskKeys.length > 0) {
        html += '<div style="margin-top:8px;display:flex;flex-direction:column;gap:4px">';
        taskKeys.forEach(function(tid) {
          var td = tasks[tid];
          var tIcon = td.status === 'completed' ? '✅' : td.status === 'failed' ? '❌' : td.status === 'skipped' ? '⏭️' : '⏳';
          var agent = td.agent ? ' 👤' + escHtml(td.agent) : '';
          var dur = td.duration_ms ? ' ' + Math.round(td.duration_ms) + 'ms' : '';
          html += '<div style="font-size:11px;padding:4px 8px;background:var(--surface2);border-radius:4px">';
          html += tIcon + ' <b>' + escHtml(tid) + '</b>' + agent + dur;
          if (td.error) html += ' <span style="color:var(--red)">⚠️ ' + escHtml(td.error.substring(0,80)) + '</span>';
          if (td.result) html += '<div style="color:var(--text3);margin-top:2px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">' + escHtml(td.result.substring(0,150)) + '</div>';
          html += '</div>';
        });
        html += '</div>';
      }
      html += '</div>';
      return html;
    }).join('');
  }).catch(function(e) { alert('Hiba: ' + e); });
}

function deleteWorkflow(wfId) {
  if (!confirm('Törlöd ezt a workflow-t?')) return;
  _skillsApiCall('/api/workflow/' + wfId, 'DELETE').then(function(d) {
    if (d.status === 'deleted') { loadWorkflows(); }
    else { alert('Hiba: ' + (d.error || 'ismeretlen')); }
  }).catch(function(e) { alert('Hiba: ' + e); });
}

function showWorkflowForm() {
  document.getElementById('workflowForm').style.display = '';
}

function loadWorkflowTemplate(tpl) {
  var templates = {
    'research': {
      name: 'research-pipeline',
      consensus: 'all',
      tasks: [
        {"id":"search","name":"Web Search","capabilities":["web_search"],"payload":{"query":"AI trends 2026"},"dependencies":[],"timeout":60},
        {"id":"analyze","name":"Analyze Results","capabilities":["data_analysis"],"dependencies":["search"],"timeout":30},
        {"id":"summarize","name":"Summarize","capabilities":["summarization"],"dependencies":["analyze"],"timeout":30}
      ]
    },
    'parallel': {
      name: 'parallel-fetch',
      consensus: 'any',
      tasks: [
        {"id":"fetch1","name":"Fetch Source 1","capabilities":["web_search"],"payload":{"query":"source A"},"dependencies":[],"timeout":30},
        {"id":"fetch2","name":"Fetch Source 2","capabilities":["web_search"],"payload":{"query":"source B"},"dependencies":[],"timeout":30},
        {"id":"fetch3","name":"Fetch Source 3","capabilities":["web_search"],"payload":{"query":"source C"},"dependencies":[],"timeout":30},
        {"id":"merge","name":"Merge Results","capabilities":["summarization"],"dependencies":["fetch1","fetch2","fetch3"],"timeout":30}
      ]
    },
    'review': {
      name: 'code-review',
      consensus: 'majority',
      tasks: [
        {"id":"review1","name":"Reviewer A","capabilities":["code_generation"],"payload":{"action":"review"},"dependencies":[],"timeout":60},
        {"id":"review2","name":"Reviewer B","capabilities":["code_generation"],"payload":{"action":"review"},"dependencies":[],"timeout":60},
        {"id":"review3","name":"Reviewer C","capabilities":["code_generation"],"payload":{"action":"review"},"dependencies":[],"timeout":60}
      ]
    },
    'conditional': {
      name: 'conditional-pipeline',
      consensus: 'all',
      tasks: [
        {"id":"search","name":"Search","capabilities":["web_search"],"payload":{"query":"latest AI news"},"dependencies":[],"timeout":60,"max_retries":2,"retry_delay":5},
        {"id":"analyze","name":"Analyze","capabilities":["data_analysis"],"dependencies":["search"],"timeout":30,"input_from":"search"},
        {"id":"deep_dive","name":"Deep Dive","capabilities":["code_generation"],"dependencies":["analyze"],"condition":"result_search and result_search.get('found') == True","timeout":60},
        {"id":"skip_note","name":"Skip Note","capabilities":["summarization"],"dependencies":["analyze"],"condition":"result_search and result_search.get('found') != True","timeout":30}
      ]
    }
  };
  var t = templates[tpl];
  if (!t) return;
  document.getElementById('wfName').value = t.name;
  document.getElementById('wfConsensus').value = t.consensus;
  document.getElementById('wfTasksJson').value = JSON.stringify(t.tasks, null, 2);
}

function createWorkflow() {
  var name = document.getElementById('wfName').value.trim();
  var consensus = document.getElementById('wfConsensus').value;
  var tasksStr = document.getElementById('wfTasksJson').value.trim();
  if (!name) { alert('Név kötelező'); return; }
  if (!tasksStr) { alert('Taskok kötelezők'); return; }
  var tasks;
  try { tasks = JSON.parse(tasksStr); } catch(e) { alert('JSON hiba: ' + e.message); return; }
  _skillsApiCall('/api/workflow', 'POST', { name: name, consensus: consensus, tasks: tasks }).then(function(d) {
    if (d.error) { alert('Hiba: ' + d.error); return; }
    alert('Workflow elindítva! ID: ' + (d.workflow_id || '?'));
    document.getElementById('workflowForm').style.display = 'none';
    document.getElementById('wfName').value = '';
    document.getElementById('wfTasksJson').value = '';
    loadWorkflows();
  }).catch(function(e) { alert('Hiba: ' + e); });
}

function checkWorkflowStatus(wfId) {
  _skillsApiCall('/api/workflow/' + wfId).then(function(d) {
    var status = d.status || '?';
    var color = status === 'completed' ? 'var(--green)' : status === 'running' ? 'var(--primary)' : status === 'failed' ? 'var(--red)' : 'var(--text3)';
    var tasks = d.tasks || [];
    var completedTasks = tasks.filter(function(t) { return t.status === 'completed'; }).length;
    var progress = tasks.length ? Math.round(completedTasks / tasks.length * 100) : 0;

    var html = '<div style="margin-bottom:12px">' +
      '<div style="font-size:14px;font-weight:600">' + escHtml(d.name || wfId) + '</div>' +
      '<div style="font-size:12px;color:var(--text3);margin-top:2px">Status: <span style="color:' + color + '">' + status + '</span> · Consensus: ' + (d.consensus || '?') + '</div>' +
      '<div style="background:var(--surface2);border-radius:4px;height:8px;overflow:hidden;margin-top:6px">' +
        '<div style="background:' + color + ';height:100%;width:' + progress + '%;transition:width .3s"></div>' +
      '</div>' +
      '<div style="font-size:11px;color:var(--text3);margin-top:3px">' + completedTasks + '/' + tasks.length + ' task completed (' + progress + '%)</div>' +
    '</div>';

    if (tasks.length) {
      html += '<div style="font-size:12px;font-weight:600;margin-bottom:8px">Tasks (DAG):</div>';
      tasks.forEach(function(t, i) {
        var ts = t.status || 'pending';
        var tc = ts === 'completed' ? 'var(--green)' : ts === 'running' ? 'var(--primary)' : ts === 'failed' ? 'var(--red)' : ts === 'timeout' ? 'var(--orange)' : ts === 'skipped' ? 'var(--text3)' : ts === 'retrying' ? 'var(--yellow)' : ts === 'pending' ? 'var(--text3)' : 'var(--yellow)';
        var icon = ts === 'completed' ? '✅' : ts === 'running' ? '🔄' : ts === 'failed' ? '❌' : ts === 'timeout' ? '⏱️' : ts === 'skipped' ? '⏭️' : ts === 'retrying' ? '🔁' : '⏳';
        var deps = t.dependencies && t.dependencies.length ? ' ← ' + t.dependencies.join(', ') : '';
        var extras = '';
        if (t.retry_count > 0) extras += ' · retry ' + t.retry_count;
        if (t.condition) extras += ' · if: ' + escHtml(t.condition.substring(0, 40));
        if (t.input_from) extras += ' ← input: ' + escHtml(t.input_from);
        html += '<div style="display:flex;align-items:center;gap:8px;padding:4px 0;border-bottom:1px solid var(--border)">' +
          '<span>' + icon + '</span>' +
          '<div style="flex:1">' +
            '<div style="font-size:13px">' + escHtml(t.name || t.id) + '<span style="color:var(--text3);font-size:11px">' + deps + '</span></div>' +
            '<div style="font-size:11px;color:' + tc + '">' + ts + (t.agent ? ' · agent: ' + escHtml(t.agent) : '') + extras + '</div>' +
          '</div>' +
        '</div>';
      });
    }

    var resultHtml = '';
    if (d.results) {
      resultHtml = '<div style="margin-top:12px;font-size:12px;font-weight:600">Results:</div>';
      Object.keys(d.results).forEach(function(key) {
        var val = d.results[key];
        var valStr = typeof val === 'string' ? val.substring(0, 200) : JSON.stringify(val).substring(0, 200);
        resultHtml += '<div style="font-size:12px;color:var(--text2);margin:4px 0;padding:4px 8px;background:var(--surface2);border-radius:4px"><b>' + escHtml(key) + '</b>: ' + escHtml(valStr) + '</div>';
      });
    }

    // Show in a modal-like overlay
    var overlay = document.createElement('div');
    overlay.style.cssText = 'position:fixed;top:0;left:0;width:100%;height:100%;background:rgba(0,0,0,.7);z-index:100;display:flex;align-items:center;justify-content:center';
    var box = document.createElement('div');
    box.style.cssText = 'background:var(--bg);border:1px solid var(--border);border-radius:12px;padding:20px;width:90vw;max-width:600px;max-height:80vh;overflow-y:auto';
    box.innerHTML = '<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px">' +
      '<h3 style="margin:0">Workflow Detail</h3>' +
      '<button style="background:none;border:none;color:var(--text);font-size:24px;cursor:pointer" onclick="this.closest(\'div[style*=fixed]\').remove()">✕</button>' +
    '</div>' + html + resultHtml;
    overlay.appendChild(box);
    overlay.onclick = function(e) { if (e.target === overlay) overlay.remove(); };
    document.body.appendChild(overlay);
  }).catch(function(e) { alert('Hiba: ' + e); });
}

// ─── Diagnostics Auto-Implement Status ─────────────────────
function loadAutoImplementStatus() {
  _skillsApiCall('/api/diagnostics/suggestions').then(function(d) {
    var suggestions = d.suggestions || [];
    var pending = suggestions.filter(function(s) { return s.status === 'pending'; });
    var accepted = suggestions.filter(function(s) { return s.status === 'accepted'; });
    var badge = document.getElementById('autoImplBadge');
    if (badge) {
      var txt = pending.length + ' pending';
      if (accepted.length) txt += ' / ' + accepted.length + ' accepted';
      badge.textContent = txt;
      badge.style.background = pending.length > 0 ? 'var(--yellow)' : 'var(--green)';
      badge.style.color = '#fff';
    }
  }).catch(function() {});
}

// ─── Alerts Modal ───────────────────────────────────────────
function showAlerts() {
  var m = document.getElementById('alertsModal');
  m.style.display = 'flex';
  loadAlerts();
}

function loadAlerts() {
  var prometheusUrl = 'http://192.168.1.100:9090';
  fetch(prometheusUrl + '/api/v1/alerts').then(function(r) { return r.json(); }).then(function(d) {
    var alerts = (d.data || {}).alerts || [];
    var c = document.getElementById('alertsList');
    document.getElementById('alertCount').textContent = alerts.length + ' alert';
    if (!alerts.length) {
      c.innerHTML = '<div style="text-align:center;padding:24px;color:var(--text3)">Nincsenek aktiv alertek ✅</div>';
      return;
    }
    c.innerHTML = alerts.map(function(a) {
      var labels = a.labels || {};
      var ann = a.annotations || {};
      var sev = labels.severity || 'info';
      var sevColor = sev === 'critical' ? 'var(--red)' : sev === 'warning' ? 'var(--yellow)' : 'var(--primary)';
      var state = a.state || '?';
      var stateColor = state === 'firing' ? 'var(--red)' : 'var(--text3)';
      return '<div style="background:var(--surface);border:1px solid var(--border);border-left:3px solid ' + sevColor + ';border-radius:8px;padding:12px;margin-bottom:8px">' +
        '<div style="display:flex;justify-content:space-between;align-items:start">' +
          '<div><span style="font-weight:600;font-size:14px">' + escHtml(labels.alertname || 'Unknown') + '</span>' +
          ' <span style="font-size:11px;padding:2px 8px;border-radius:10px;background:' + stateColor + ';color:#fff">' + state + '</span></div>' +
          '<span style="font-size:11px;padding:2px 8px;border-radius:10px;background:' + sevColor + ';color:#fff">' + sev + '</span>' +
        '</div>' +
        (labels.node ? '<div style="font-size:12px;color:var(--text3);margin-top:4px">node: ' + escHtml(labels.node) + '</div>' : '') +
        (ann.summary ? '<div style="font-size:13px;color:var(--text2);margin-top:4px">' + escHtml(ann.summary) + '</div>' : '') +
        (ann.description ? '<div style="font-size:12px;color:var(--text3);margin-top:2px">' + escHtml(ann.description) + '</div>' : '') +
      '</div>';
    }).join('');
  }).catch(function(e) {
    document.getElementById('alertsList').innerHTML = '<div style="text-align:center;padding:24px;color:var(--text3)">Prometheus nem elerheto — probald a Custom Rules tabot</div>';
  });
}

function switchAlertTab(tab) {
  var prom = document.getElementById('alertsList');
  var rules = document.getElementById('alertRulesPanel');
  var mesh = document.getElementById('alertMeshPanel');
  var btnProm = document.getElementById('alertTabProm');
  var btnRules = document.getElementById('alertTabRules');
  var btnMesh = document.getElementById('alertTabMesh');
  if (tab === 'prom') {
    prom.style.display = ''; rules.style.display = 'none'; mesh.style.display = 'none';
    btnProm.style.background = 'var(--primary)'; btnProm.style.color = '#fff';
    btnRules.style.background = 'var(--surface2)'; btnRules.style.color = 'var(--text)';
    btnMesh.style.background = 'var(--surface2)'; btnMesh.style.color = 'var(--text)';
    loadAlerts();
  } else if (tab === 'rules') {
    prom.style.display = 'none'; rules.style.display = ''; mesh.style.display = 'none';
    btnProm.style.background = 'var(--surface2)'; btnProm.style.color = 'var(--text)';
    btnRules.style.background = 'var(--primary)'; btnRules.style.color = '#fff';
    btnMesh.style.background = 'var(--surface2)'; btnMesh.style.color = 'var(--text)';
    loadAlertRules();
  } else if (tab === 'mesh') {
    prom.style.display = 'none'; rules.style.display = 'none'; mesh.style.display = '';
    btnProm.style.background = 'var(--surface2)'; btnProm.style.color = 'var(--text)';
    btnRules.style.background = 'var(--surface2)'; btnRules.style.color = 'var(--text)';
    btnMesh.style.background = 'var(--primary)'; btnMesh.style.color = '#fff';
    loadMeshAlerts();
  }
}

function loadMeshAlerts() {
  _skillsApiCall('/api/alerts/delegation').then(function(d) {
    var c = document.getElementById('alertMeshList');
    var alerts = d.alerts || [];
    var sevBadges = '';
    if (d.critical > 0) sevBadges += '<span style="background:var(--red);color:#fff;font-size:11px;padding:2px 8px;border-radius:10px;margin-right:4px">' + d.critical + ' critical</span>';
    if (d.warning > 0) sevBadges += '<span style="background:var(--yellow);color:#000;font-size:11px;padding:2px 8px;border-radius:10px;margin-right:4px">' + d.warning + ' warning</span>';
    if (d.info > 0) sevBadges += '<span style="background:var(--primary);color:#fff;font-size:11px;padding:2px 8px;border-radius:10px">' + d.info + ' info</span>';
    document.getElementById('alertCount').textContent = d.total + ' alert';
    if (!alerts.length) {
      c.innerHTML = '<div style="text-align:center;padding:24px;color:var(--text3)">✅ Nincsenek mesh alertek — minden rendben</div>';
      return;
    }
    var icons = {critical: '🔴', warning: '🟡', info: '🔵'};
    var colors = {critical: 'var(--red)', warning: 'var(--yellow)', info: 'var(--primary)'};
    var sourceIcons = {delegation: '📤', kanban: '📋', system: '⚙️'};
    c.innerHTML = sevBadges + '<div style="margin-top:8px"></div>' + alerts.map(function(a) {
      var sev = a.severity || 'info';
      var src = a.source || 'system';
      return '<div style="background:var(--surface);border:1px solid var(--border);border-left:3px solid ' + colors[sev] + ';border-radius:8px;padding:12px;margin-bottom:8px">' +
        '<div style="display:flex;justify-content:space-between;align-items:start">' +
          '<div><span style="font-weight:600;font-size:14px">' + (icons[sev]||'⚪') + ' ' + escHtml(a.title||'') + '</span>' +
          ' <span style="font-size:10px;padding:1px 6px;border-radius:8px;background:var(--surface2);color:var(--text3)">' + (sourceIcons[src]||'') + ' ' + src + '</span></div>' +
          '<span style="font-size:11px;color:var(--text3)">' + escHtml(a.age||'') + '</span>' +
        '</div>' +
        '<div style="font-size:12px;color:var(--text2);margin-top:4px">' + escHtml(a.message||'') + '</div>' +
        (a.task_id ? '<div style="font-size:10px;color:var(--text3);margin-top:2px">task: ' + a.task_id.substring(0,12) + '</div>' : '') +
        (a.card_id ? '<div style="font-size:10px;color:var(--text3);margin-top:2px">card: ' + a.card_id.substring(a.card_id.length-4) + '</div>' : '') +
      '</div>';
    }).join('');
  }).catch(function(e) {
    document.getElementById('alertMeshList').innerHTML = '<div style="text-align:center;padding:24px;color:var(--danger)">Hiba: ' + escHtml(String(e)) + '</div>';
  });
}

function loadAlertRules() {
  _skillsApiCall('/api/alerts').then(function(d) {
    var c = document.getElementById('alertRulesList');
    document.getElementById('alertCount').textContent = (d.total_rules || 0) + ' rule';
    var rules = d.rules || [];
    if (!rules.length) {
      c.innerHTML = '<div style="text-align:center;padding:24px;color:var(--text3)">Nincsenek custom alert rules</div>';
      return;
    }
    c.innerHTML = rules.map(function(r) {
      var sevColor = r.severity === 'critical' ? 'var(--red)' : r.severity === 'warning' ? 'var(--yellow)' : 'var(--primary)';
      var stateColor = r.state === 'firing' ? 'var(--red)' : r.state === 'resolved' ? 'var(--green)' : 'var(--text3)';
      var enabledBtn = r.enabled
        ? '<button class="btn btn-sm" style="background:var(--green);color:#fff;font-size:11px" onclick="toggleAlertRule(\'' + r.id + '\')">ON</button>'
        : '<button class="btn btn-sm" style="background:var(--surface2);color:var(--text3);font-size:11px" onclick="toggleAlertRule(\'' + r.id + '\')">OFF</button>';
      return '<div style="background:var(--surface);border:1px solid var(--border);border-left:3px solid ' + sevColor + ';border-radius:8px;padding:12px;margin-bottom:8px">' +
        '<div style="display:flex;justify-content:space-between;align-items:center">' +
          '<div><span style="font-weight:600;font-size:13px">' + escHtml(r.name) + '</span>' +
          ' <span style="font-size:11px;padding:2px 8px;border-radius:10px;background:' + sevColor + ';color:#fff">' + r.severity + '</span>' +
          ' <span style="font-size:11px;padding:2px 8px;border-radius:10px;background:' + stateColor + ';color:#fff">' + r.state + '</span></div>' +
          '<div style="display:flex;gap:4px">' + enabledBtn +
          ' <button class="btn btn-sm" style="background:var(--red);color:#fff;font-size:11px" onclick="deleteAlertRule(\'' + r.id + '\')">Törlés</button></div>' +
        '</div>' +
        '<div style="font-size:12px;color:var(--text3);margin-top:4px">' + escHtml(r.metric) + ' ' + escHtml(r.operator) + ' ' + r.threshold + ' · cooldown: ' + r.cooldown + 's · fires: ' + r.fire_count + '</div>' +
      '</div>';
    }).join('');
  }).catch(function(e) {
    document.getElementById('alertRulesList').innerHTML = '<div style="text-align:center;padding:24px;color:var(--text3)">Hiba: ' + escHtml(String(e)) + '</div>';
  });
}

function addAlertRule() {
  var id = document.getElementById('arId').value.trim();
  var name = document.getElementById('arName').value.trim();
  var metric = document.getElementById('arMetric').value.trim();
  var op = document.getElementById('arOp').value;
  var threshold = parseFloat(document.getElementById('arThreshold').value || '0');
  var severity = document.getElementById('arSeverity').value;
  var cooldown = parseFloat(document.getElementById('arCooldown').value || '300');
  if (!id || !metric) { alert('ID es metrika kotelezo'); return; }
  _skillsApiCall('/api/alerts/rules', 'POST', { id: id, name: name || id, metric: metric, operator: op, threshold: threshold, severity: severity, cooldown: cooldown }).then(function(d) {
    if (d.error) { alert('Hiba: ' + d.error); return; }
    document.getElementById('arId').value = ''; document.getElementById('arName').value = ''; document.getElementById('arMetric').value = ''; document.getElementById('arThreshold').value = '';
    loadAlertRules();
  }).catch(function(e) { alert('Hiba: ' + e); });
}

function deleteAlertRule(ruleId) {
  _skillsApiCall('/api/alerts/rules/' + ruleId, 'DELETE').then(function(d) {
    loadAlertRules();
  }).catch(function(e) { alert('Hiba: ' + e); });
}

function toggleAlertRule(ruleId) {
  _skillsApiCall('/api/alerts/rules/' + ruleId + '/toggle', 'POST').then(function(d) {
    loadAlertRules();
  }).catch(function(e) { alert('Hiba: ' + e); });
}

// ─── Topology Modal ──────────────────────────────────────────
function showTopology() {
  var m = document.getElementById('topologyModal');
  m.style.display = 'flex';
  var frame = document.getElementById('topologyFrame');
  frame.src = '/topology?t=' + Date.now();
}

// ─── Diagnostics Modal ─────────────────────────────────────────
var _diagReports = [];
var _diagSuggestions = [];

function showDiagnostics() {
  var m = document.getElementById('diagnosticsModal');
  m.style.display = 'flex';
  loadDiagReports();
  loadDiagSuggestions();
  loadAutoImplementStatus();
  document.getElementById('diagReportDetail').style.display = 'none';
  document.getElementById('diagReportsSection').style.display = '';
}

function switchDiagTab(tab) {
  var rBtn = document.getElementById('diagTabReports');
  var sBtn = document.getElementById('diagTabSuggestions');
  var rSec = document.getElementById('diagReportsSection');
  var sSec = document.getElementById('diagSuggestionsSection');
  var det = document.getElementById('diagReportDetail');
  det.style.display = 'none';
  if (tab === 'reports') {
    rSec.style.display = ''; sSec.style.display = 'none';
    rBtn.style.background = 'var(--primary)'; rBtn.style.color = '#fff';
    sBtn.style.background = 'var(--surface2)'; sBtn.style.color = 'var(--text)';
  } else {
    rSec.style.display = 'none'; sSec.style.display = '';
    sBtn.style.background = 'var(--primary)'; sBtn.style.color = '#fff';
    rBtn.style.background = 'var(--surface2)'; rBtn.style.color = 'var(--text)';
  }
}

var _diagToken = null;
function _diagAuth() {
  if (_diagToken) return Promise.resolve(_diagToken);
  return fetch('/api/auth/login', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({username:'zsolt',password:'mesh2026'})})
    .then(function(r){return r.json()}).then(function(d){_diagToken=d.token; return _diagToken;});
}

// ─── Report list ─────────────────────────────────────────
function loadDiagReports() {
  var el = document.getElementById('diagReportsList');
  el.innerHTML = '<div style="text-align:center;padding:20px">⏳ Betöltés...</div>';
  _diagAuth().then(function(token) {
    return fetch('/api/diagnostics/reports', {headers:{'Authorization':'Bearer '+token}});
  }).then(function(r){return r.json()}).then(function(data) {
    _diagReports = data.reports || [];
    if (_diagReports.length === 0) {
      el.innerHTML = '<div style="text-align:center;padding:20px;color:var(--text3)">Nincs diagnostic report</div>';
      return;
    }
    var html = '<div style="display:flex;flex-direction:column;gap:8px">';
    _diagReports.forEach(function(r, idx) {
      var sevColor = r.severity === 'critical' ? '#ef4444' : r.severity === 'warning' ? '#f59e0b' : '#10b981';
      var sevIcon = r.severity === 'critical' ? '🔴' : r.severity === 'warning' ? '🟡' : '🟢';
      html += '<div style="background:var(--surface2);border:1px solid var(--border);border-radius:8px;padding:10px 14px;cursor:pointer" onclick="showDiagReportDetail('+idx+')">';
      html += '<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:4px">';
      html += '<span style="font-weight:600;font-size:13px">' + sevIcon + ' ' + (r.node || '?') + ' — ' + (r.report_type || 'report') + '</span>';
      html += '<span style="font-size:11px;color:var(--text3)">' + (r.timestamp ? r.timestamp.substring(5,16).replace('T',' ') : '') + '</span>';
      html += '</div>';
      html += '<div style="font-size:12px;color:var(--text2)">' + (r.summary || r.report_id || '') + '</div>';
      if (r.recommendations && r.recommendations.length > 0) {
        html += '<div style="font-size:11px;color:var(--text3);margin-top:4px">💡 ' + r.recommendations.slice(0,2).join(' · ') + '</div>';
      }
      html += '</div>';
    });
    html += '</div>';
    el.innerHTML = html;
  }).catch(function(e) { el.innerHTML = '<div style="color:#ef4444">❌ Hiba: '+e.message+'</div>'; });
}

// ─── Report detail view ──────────────────────────────────
function showDiagReportDetail(idx) {
  var r = _diagReports[idx];
  if (!r) return;
  var sevColor = r.severity === 'critical' ? '#ef4444' : r.severity === 'warning' ? '#f59e0b' : '#10b981';
  var sevLabel = r.severity === 'critical' ? 'Kritikus' : r.severity === 'warning' ? 'Figyelmeztetés' : 'Info';
  var html = '';
  // Header
  html += '<div style="background:var(--surface2);border-radius:8px;padding:14px;margin-bottom:12px;border-left:4px solid '+sevColor+'">';
  html += '<div style="display:flex;justify-content:space-between;align-items:center">';
  html += '<h3 style="margin:0;font-size:15px">' + (r.node || '?') + ' — ' + (r.report_type || 'report') + '</h3>';
  html += '<span style="background:'+sevColor+';color:#fff;padding:2px 10px;border-radius:12px;font-size:11px;font-weight:600">'+sevLabel+'</span>';
  html += '</div>';
  html += '<div style="font-size:11px;color:var(--text3);margin-top:4px">'+(r.timestamp||'')+' · ID: '+(r.report_id||'')+'</div>';
  html += '</div>';
  // Summary
  if (r.summary) {
    html += '<div style="background:var(--surface2);border-radius:8px;padding:12px;margin-bottom:10px">';
    html += '<div style="font-size:12px;font-weight:600;color:var(--text2);margin-bottom:6px">📝 Összegzés</div>';
    html += '<div style="font-size:13px;color:var(--text)">'+r.summary+'</div>';
    html += '</div>';
  }
  // Sections
  var sections = [
    {key:'memory_stats', icon:'💾', label:'Memória & Rendszer'},
    {key:'error_patterns', icon:'🐛', label:'Hibák'},
    {key:'mesh_health', icon:'🌐', label:'Mesh állapot'},
    {key:'performance', icon:'⚡', label:'Teljesítmény'}
  ];
  sections.forEach(function(sec) {
    var data = r[sec.key];
    if (data && typeof data === 'object' && Object.keys(data).length > 0) {
      html += '<div style="background:var(--surface2);border-radius:8px;padding:12px;margin-bottom:8px">';
      html += '<div style="font-size:12px;font-weight:600;color:var(--text2);margin-bottom:8px">'+sec.icon+' '+sec.label+'</div>';
      html += _renderDetailObj(data);
      html += '</div>';
    }
  });
  // Recommendations
  if (r.recommendations && r.recommendations.length > 0) {
    html += '<div style="background:var(--surface2);border-radius:8px;padding:12px;margin-bottom:8px">';
    html += '<div style="font-size:12px;font-weight:600;color:var(--text2);margin-bottom:8px">💡 Javaslatok</div>';
    r.recommendations.forEach(function(rec) {
      html += '<div style="font-size:13px;color:var(--text);padding:4px 0;border-bottom:1px solid var(--border)">• '+rec+'</div>';
    });
    html += '</div>';
  }
  document.getElementById('diagReportDetailContent').innerHTML = html;
  document.getElementById('diagReportsSection').style.display = 'none';
  document.getElementById('diagReportDetail').style.display = '';
}

function hideDiagReportDetail() {
  document.getElementById('diagReportDetail').style.display = 'none';
  document.getElementById('diagReportsSection').style.display = '';
}

function _renderDetailObj(obj, depth) {
  depth = depth || 0;
  if (typeof obj !== 'object' || obj === null) return '<span style="color:var(--text)">'+String(obj)+'</span>';
  var html = '<div style="margin-left:'+(depth*12)+'px">';
  Object.keys(obj).forEach(function(k) {
    var v = obj[k];
    var label = k.replace(/_/g, ' ').replace(/\b\w/g, function(c){return c.toUpperCase()});
    if (typeof v === 'object' && v !== null && !Array.isArray(v)) {
      html += '<div style="margin-bottom:4px"><span style="font-size:11px;font-weight:600;color:var(--text3)">'+label+'</span></div>';
      html += _renderDetailObj(v, depth+1);
    } else if (Array.isArray(v)) {
      html += '<div style="display:flex;justify-content:space-between;padding:3px 0;border-bottom:1px solid var(--border)">';
      html += '<span style="font-size:12px;color:var(--text3)">'+label+'</span>';
      html += '<span style="font-size:12px;color:var(--text)">'+v.length+' elem</span>';
      html += '</div>';
    } else {
      var valColor = typeof v === 'number' ? (v > 80 ? '#ef4444' : v > 50 ? '#f59e0b' : '#10b981') : 'var(--text)';
      if (typeof v === 'string' && (v === 'true' || v === 'connected' || v === 'healthy')) valColor = '#10b981';
      if (typeof v === 'string' && (v === 'false' || v === 'disconnected' || v === 'unhealthy')) valColor = '#ef4444';
      html += '<div style="display:flex;justify-content:space-between;padding:3px 0;border-bottom:1px solid var(--border)">';
      html += '<span style="font-size:12px;color:var(--text3)">'+label+'</span>';
      html += '<span style="font-size:12px;color:'+valColor+';font-weight:500">'+String(v)+'</span>';
      html += '</div>';
    }
  });
  html += '</div>';
  return html;
}

// ─── Suggestions list ────────────────────────────────────
function filterSuggestions(status) {
  loadDiagSuggestions(status);
}

function loadDiagSuggestions(filter) {
  var el = document.getElementById('diagSuggestionsList');
  el.innerHTML = '<div style="text-align:center;padding:20px">⏳ Betöltés...</div>';
  var url = '/api/diagnostics/suggestions?limit=50';
  if (filter && filter !== 'all') url += '&status=' + filter;
  _diagAuth().then(function(token) {
    return fetch(url, {headers:{'Authorization':'Bearer '+token}});
  }).then(function(r){return r.json()}).then(function(data) {
    _diagSuggestions = data.suggestions || [];
    if (_diagSuggestions.length === 0) {
      el.innerHTML = '<div style="text-align:center;padding:20px;color:var(--text3)">Nincs javaslat</div>';
      return;
    }
    var html = '<div style="display:flex;flex-direction:column;gap:10px">';
    _diagSuggestions.forEach(function(s, idx) {
      var prColor = s.priority === 'critical' ? '#ef4444' : s.priority === 'high' ? '#f59e0b' : s.priority === 'medium' ? '#3b82f6' : '#10b981';
      var prIcon = s.priority === 'critical' ? '🔴' : s.priority === 'high' ? '🟠' : s.priority === 'medium' ? '🔵' : '🟢';
      var stLabel = s.status || 'pending';
      var stColor = stLabel === 'implemented' ? '#10b981' : stLabel === 'accepted' ? '#3b82f6' : stLabel === 'rejected' ? '#ef4444' : '#f59e0b';
      var stIcon = stLabel === 'implemented' ? '✅' : stLabel === 'accepted' ? '👍' : stLabel === 'rejected' ? '❌' : '⏳';
      var stText = stLabel === 'implemented' ? 'Megvalósítva' : stLabel === 'accepted' ? 'Elfogadva' : stLabel === 'rejected' ? 'Elutasítva' : 'Függőben';
      html += '<div style="background:var(--surface2);border-left:4px solid '+prColor+';border-radius:8px;padding:12px 14px">';
      // Title row
      html += '<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:6px">';
      html += '<span style="font-weight:600;font-size:14px">'+prIcon+' '+(s.title||'Javaslat')+'</span>';
      html += '<span style="background:'+stColor+';color:#fff;padding:2px 10px;border-radius:12px;font-size:11px;font-weight:600">'+stIcon+' '+stText+'</span>';
      html += '</div>';
      // Meta row
      html += '<div style="display:flex;gap:12px;font-size:11px;color:var(--text3);margin-bottom:6px">';
      html += '<span>📂 '+(s.category||'általános')+'</span>';
      html += '<span>📡 '+(s.node||'?')+'</span>';
      if (s.affected_nodes && s.affected_nodes.length > 0) {
        html += '<span>🔗 '+s.affected_nodes.join(', ')+'</span>';
      }
      html += '<span>'+(s.timestamp?s.timestamp.substring(5,16).replace('T',' '):'')+'</span>';
      html += '</div>';
      // Description
      if (s.description) {
        html += '<div style="font-size:13px;color:var(--text2);margin-bottom:8px">'+s.description+'</div>';
      }
      // Values
      if (s.current_value || s.suggested_value) {
        html += '<div style="display:flex;gap:16px;margin-bottom:8px">';
        if (s.current_value) {
          html += '<div style="flex:1;background:var(--surface);border-radius:6px;padding:8px"><div style="font-size:10px;color:var(--text3);margin-bottom:2px">Jelenlegi érték</div><code style="font-size:12px;color:var(--text)">'+s.current_value+'</code></div>';
        }
        if (s.suggested_value) {
          html += '<div style="flex:1;background:var(--surface);border-radius:6px;padding:8px;border:1px solid var(--primary)"><div style="font-size:10px;color:var(--primary);margin-bottom:2px">Javasolt érték</div><code style="font-size:12px;color:var(--text)">'+s.suggested_value+'</code></div>';
        }
        html += '</div>';
      }
      // Rationale
      if (s.rationale) {
        html += '<div style="font-size:12px;color:var(--text3);font-style:italic;margin-bottom:8px">💭 '+s.rationale+'</div>';
      }
      // Action buttons
      html += '<div style="display:flex;gap:6px;flex-wrap:wrap">';
      if (stLabel === 'pending') {
        html += '<button class="btn btn-sm" style="background:#3b82f6;color:#fff;font-size:11px;padding:3px 12px" onclick="updateSuggestionStatus(\''+s.suggestion_id+'\',\'accepted\','+idx+')">👍 Elfogad</button>';
        html += '<button class="btn btn-sm" style="background:#ef4444;color:#fff;font-size:11px;padding:3px 12px" onclick="updateSuggestionStatus(\''+s.suggestion_id+'\',\'rejected\','+idx+')">❌ Elutasít</button>';
      } else if (stLabel === 'accepted') {
        html += '<button class="btn btn-sm" style="background:#10b981;color:#fff;font-size:11px;padding:3px 12px" onclick="updateSuggestionStatus(\''+s.suggestion_id+'\',\'implemented\','+idx+')">✅ Megvalósítva</button>';
        html += '<button class="btn btn-sm" style="background:#6b7280;color:#fff;font-size:11px;padding:3px 12px" onclick="updateSuggestionStatus(\''+s.suggestion_id+'\',\'rejected\','+idx+')">❌ Elutasít</button>';
      } else if (stLabel === 'implemented') {
        html += '<span style="font-size:11px;color:#10b981">✅ Megvalósítva</span>';
      } else if (stLabel === 'rejected') {
        html += '<button class="btn btn-sm" style="background:#3b82f6;color:#fff;font-size:11px;padding:3px 12px" onclick="updateSuggestionStatus(\''+s.suggestion_id+'\',\'pending\','+idx+')">↩ Újra megnyit</button>';
      }
      html += '</div>';
      html += '</div>';
    });
    html += '</div>';
    el.innerHTML = html;
  }).catch(function(e) { el.innerHTML = '<div style="color:#ef4444">❌ Hiba: '+e.message+'</div>'; });
}


// ─── Suggestion detail view ──────────────────────────────────
function showSuggestionDetail(idx) {
  var s = _diagSuggestions[idx];
  if (!s) return;
  var prColor = s.priority === 'critical' ? '#ef4444' : s.priority === 'high' ? '#f59e0b' : s.priority === 'medium' ? '#3b82f6' : '#10b981';
  var prIcon = s.priority === 'critical' ? '🔴' : s.priority === 'high' ? '🟠' : s.priority === 'medium' ? '🔵' : '🟢';
  var prLabel = s.priority === 'critical' ? 'Kritikus' : s.priority === 'high' ? 'Magas' : s.priority === 'medium' ? 'Közepes' : 'Alacsony';
  var stLabel = s.status || 'pending';
  var stColor = stLabel === 'implemented' ? '#10b981' : stLabel === 'accepted' ? '#3b82f6' : stLabel === 'rejected' ? '#ef4444' : '#f59e0b';
  var stIcon = stLabel === 'implemented' ? '✅' : stLabel === 'accepted' ? '👍' : stLabel === 'rejected' ? '❌' : '⏳';
  var stText = stLabel === 'implemented' ? 'Megvalósítva' : stLabel === 'accepted' ? 'Elfogadva' : stLabel === 'rejected' ? 'Elutasítva' : 'Függőben';
  var catIcons = {memory:'💾',performance:'⚡',network:'🌐',storage:'💿',stability:'🔧',security:'🔒',general:'📋'};
  var catIcon = catIcons[s.category] || '📋';
  var html = '';
  // Back button
  html += '<div style="margin-bottom:12px"><button class="btn btn-sm" style="background:var(--surface2);color:var(--text);font-size:12px;padding:4px 12px" onclick="loadDiagSuggestions()">← Vissza a javaslatokhoz</button></div>';
  // Header card
  html += '<div style="background:var(--surface2);border-radius:8px;padding:14px;margin-bottom:12px;border-left:4px solid '+prColor+'">';
  html += '<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px">';
  html += '<h3 style="margin:0;font-size:15px">'+prIcon+' '+(s.title||'Javaslat')+'</h3>';
  html += '<span style="background:'+stColor+';color:#fff;padding:4px 12px;border-radius:12px;font-size:12px;font-weight:600">'+stIcon+' '+stText+'</span>';
  html += '</div>';
  html += '<div style="display:flex;gap:16px;font-size:12px;color:var(--text3)">';
  html += '<span>'+prIcon+' '+prLabel+'</span>';
  html += '<span>'+catIcon+' '+(s.category||'általános')+'</span>';
  html += '<span>📡 '+(s.node||'?')+'</span>';
  if (s.affected_nodes && s.affected_nodes.length > 0) {
    html += '<span>🔗 '+s.affected_nodes.join(', ')+'</span>';
  }
  html += '</div>';
  html += '<div style="font-size:11px;color:var(--text3);margin-top:4px">'+(s.timestamp?s.timestamp.replace('T',' '):'')+' · ID: '+(s.suggestion_id||'')+'</div>';
  html += '</div>';
  // Description card
  if (s.description) {
    html += '<div style="background:var(--surface2);border-radius:8px;padding:12px;margin-bottom:10px">';
    html += '<div style="font-size:12px;font-weight:600;color:var(--text2);margin-bottom:6px">📝 Leírás</div>';
    html += '<div style="font-size:13px;color:var(--text);line-height:1.5">'+s.description+'</div>';
    html += '</div>';
  }
  // Values card
  if (s.current_value || s.suggested_value) {
    html += '<div style="background:var(--surface2);border-radius:8px;padding:12px;margin-bottom:10px">';
    html += '<div style="font-size:12px;font-weight:600;color:var(--text2);margin-bottom:8px">📊 Értékek</div>';
    html += '<div style="display:flex;gap:12px">';
    if (s.current_value) {
      html += '<div style="flex:1;background:var(--surface);border-radius:6px;padding:10px">';
      html += '<div style="font-size:10px;color:var(--text3);margin-bottom:4px">Jelenlegi érték</div>';
      html += '<div style="font-size:14px;font-weight:600;color:var(--text)">'+s.current_value+'</div>';
      html += '</div>';
    }
    if (s.suggested_value) {
      html += '<div style="flex:1;background:var(--surface);border-radius:6px;padding:10px;border:1px solid var(--primary)">';
      html += '<div style="font-size:10px;color:var(--primary);margin-bottom:4px">Javasolt érték</div>';
      html += '<div style="font-size:14px;font-weight:600;color:var(--text)">'+s.suggested_value+'</div>';
      html += '</div>';
    }
    html += '</div>';
    html += '</div>';
  }
  // Rationale card
  if (s.rationale) {
    html += '<div style="background:var(--surface2);border-radius:8px;padding:12px;margin-bottom:10px">';
    html += '<div style="font-size:12px;font-weight:600;color:var(--text2);margin-bottom:6px">💭 Indoklás</div>';
    html += '<div style="font-size:13px;color:var(--text);line-height:1.5;font-style:italic">'+s.rationale+'</div>';
    html += '</div>';
  }
  // Error context card (if available in description)
  if (s.description && (s.description.toLowerCase().includes('hiba') || s.description.toLowerCase().includes('error') || s.description.toLowerCase().includes('OOM'))) {
    html += '<div style="background:#1a1a2e;border:1px solid #ef4444;border-radius:8px;padding:12px;margin-bottom:10px">';
    html += '<div style="font-size:12px;font-weight:600;color:#ef4444;margin-bottom:6px">⚠️ Észlelt hiba</div>';
    html += '<div style="font-size:13px;color:var(--text);line-height:1.5">'+s.description+'</div>';
    html += '<div style="font-size:11px;color:var(--text3);margin-top:6px">💡 Javasolt megoldás: '+(s.suggested_value||s.rationale||'—')+'</div>';
    html += '</div>';
  }
  // Action buttons card
  html += '<div style="background:var(--surface2);border-radius:8px;padding:12px">';
  html += '<div style="font-size:12px;font-weight:600;color:var(--text2);margin-bottom:8px">🎯 Műveletek</div>';
  html += '<div style="display:flex;gap:8px;flex-wrap:wrap">';
  if (stLabel === 'pending') {
    html += '<button class="btn btn-sm" style="background:#3b82f6;color:#fff;font-size:12px;padding:6px 16px" onclick="updateSuggestionStatus(\''+s.suggestion_id+'\',\'accepted\','+idx+')">👍 Elfogad</button>';
    html += '<button class="btn btn-sm" style="background:#ef4444;color:#fff;font-size:12px;padding:6px 16px" onclick="updateSuggestionStatus(\''+s.suggestion_id+'\',\'rejected\','+idx+')">❌ Elutasít</button>';
  } else if (stLabel === 'accepted') {
    html += '<button class="btn btn-sm" style="background:#10b981;color:#fff;font-size:12px;padding:6px 16px" onclick="updateSuggestionStatus(\''+s.suggestion_id+'\',\'implemented\','+idx+')">✅ Megvalósítva</button>';
    html += '<button class="btn btn-sm" style="background:#6b7280;color:#fff;font-size:12px;padding:6px 16px" onclick="updateSuggestionStatus(\''+s.suggestion_id+'\',\'rejected\','+idx+')">❌ Elutasít</button>';
  } else if (stLabel === 'implemented') {
    html += '<span style="font-size:12px;color:#10b981;font-weight:600">✅ Megvalósítva</span>';
    html += '<button class="btn btn-sm" style="background:var(--surface);color:var(--text);font-size:12px;padding:6px 16px" onclick="updateSuggestionStatus(\''+s.suggestion_id+'\',\'pending\','+idx+')">↩ Újra megnyit</button>';
  } else if (stLabel === 'rejected') {
    html += '<button class="btn btn-sm" style="background:#3b82f6;color:#fff;font-size:12px;padding:6px 16px" onclick="updateSuggestionStatus(\''+s.suggestion_id+'\',\'pending\','+idx+')">↩ Újra megnyit</button>';
  }
  // Auto-implement button
  if (stLabel === 'pending' || stLabel === 'accepted') {
    html += '<button class="btn btn-sm" style="background:#8b5cf6;color:#fff;font-size:12px;padding:6px 16px" onclick="autoImplementSuggestion(\''+s.suggestion_id+'\','+idx+')">🤖 Auto-megvalósítás</button>';
  }
  html += '</div>';
  html += '</div>';
  var el = document.getElementById('diagSuggestionsList');
  if (el) el.innerHTML = html;
}

function autoImplementSuggestion(id, idx) {
  _diagAuth().then(function(token) {
    return fetch('/api/diagnostics/auto-implement', {
      method:'POST',
      headers:{'Authorization':'Bearer '+token, 'Content-Type':'application/json'},
      body:JSON.stringify({})
    });
  }).then(function(r){return r.json()}).then(function(data) {
    if (data.implemented !== undefined) {
      alert('Auto-implement: '+data.implemented+' javaslat megvalósítva');
      loadDiagSuggestions();
    } else {
      alert('Hiba: '+(data.error||'Ismeretlen'));
    }
  }).catch(function(e) { alert('Hiba: '+e.message); });
}

function updateSuggestionStatus(id, status, idx) {
  _diagAuth().then(function(token) {
    return fetch('/api/diagnostics/suggestions/'+encodeURIComponent(id), {
      method:'PATCH',
      headers:{'Authorization':'Bearer '+token, 'Content-Type':'application/json'},
      body:JSON.stringify({status:status})
    });
  }).then(function(r){return r.json()}).then(function(data) {
    if (data.suggestion_id) {
      loadDiagSuggestions();
    } else {
      alert('Hiba: '+(data.error||'Ismeretlen'));
    }
  }).catch(function(e) { alert('Hiba: '+e.message); });
}

function generateDiagReport() {
  var btn = event.target;
  btn.disabled = true; btn.textContent = '⏳ Generálás...';
  _diagAuth().then(function(token) {
    return fetch('/api/diagnostics/report', {method:'POST', headers:{'Authorization':'Bearer '+token}});
  }).then(function(r){return r.json()}).then(function(data) {
    btn.disabled = false; btn.textContent = '🔄 Új report';
    if (data.report_id) {
      loadDiagReports();
      switchDiagTab('reports');
      hideDiagReportDetail();
    } else {
      alert('Hiba: ' + (data.error || 'Ismeretlen hiba'));
    }
  }).catch(function(e) {
    btn.disabled = false; btn.textContent = '🔄 Új report';
    alert('Hiba: ' + e.message);
  });
}

// ── Inline Kanban (no iframe) ──
var _inlineKanbanBoards = [];
var _inlineKanbanCurrent = null;



// Auto-refresh inline Kanban when modal is open
var _kanbanAutoRefresh = null;




function renderInlineKanban() {
  var board = _inlineKanbanBoards.find(function(b) { return b.id === _inlineKanbanCurrent; });
  if (!board) return;
  var area = document.getElementById('inlineKanbanArea');
  var cards = board.cards || [];
  var cols = board.columns || ["todo","in_progress","review","done"];
  var colLabels = {todo:"📋 Teendő", in_progress:"⚙️ Folyamatban", review:"👁️ Ellenőrzés", done:"✅ Kész"};
  var colColors = {todo:"var(--text3)", in_progress:"var(--warning)", review:"#8b5cf6", done:"var(--success)"};
  
  area.innerHTML = cols.map(function(col) {
    var colCards = cards.filter(function(c) { return c.column === col; });
    var headerStyle = "background:" + (colColors[col] || "var(--info)") + ";color:#fff;padding:8px;border-radius:8px 8px 0 0;font-size:13px;font-weight:700;text-align:center";
    var html = '<div style="flex:1;min-width:200px;background:var(--surface);border-radius:8px;border:1px solid var(--border);display:flex;flex-direction:column;max-height:100%">';
    html += '<div style="' + headerStyle + '">' + (colLabels[col] || col) + ' <span style="background:rgba(255,255,255,.2);padding:1px 6px;border-radius:8px">' + colCards.length + '</span></div>';
    html += '<div style="padding:6px;overflow-y:auto;flex:1;display:flex;flex-direction:column;gap:6px">';
    colCards.forEach(function(card) {
      var delBadge = card.delegation_task_id ? ' 🔗' : '';
      var apprBadge = card.approval_required ? ' ⏳' : '';
      var priColor = card.priority >= 8 ? 'var(--danger)' : card.priority >= 5 ? 'var(--warning)' : 'var(--text3)';
      html += '<div style="background:var(--surface2);border:1px solid var(--border);border-radius:6px;padding:8px;font-size:12px;cursor:pointer" onclick="showInlineCardDetail(\'' + board.id + '\',\'' + card.id + '\')">';
      html += '<div style="font-weight:600;font-size:13px;margin-bottom:4px">' + escHtml(card.title || '') + delBadge + apprBadge + '</div>';
      if (card.assigned_to) html += '<div style="color:var(--text3);font-size:11px">👤 ' + escHtml(card.assigned_to) + '</div>';
      if (card.delegation_status === 'completed') html += '<div style="color:var(--success);font-size:10px">✅ completed</div>';
      if (card.agent_history && card.agent_history.length > 0) {
        var agentBadges = card.agent_history.map(function(h) {
          var icon = h.role === 'delegator' ? '📤' : h.role === 'executor' ? '⚙️' : h.role === 'reviewer' ? '🔍' : '👤';
          var color = h.verdict === 'accept' ? 'var(--success)' : h.verdict === 'reject' ? 'var(--danger)' : 'var(--text3)';
          return '<span style="color:' + color + '">' + icon + ' ' + escHtml(h.agent || '') + '</span>';
        }).join(' · ');
        html += '<div style="font-size:10px;color:var(--text3);margin-top:2px">' + agentBadges + '</div>';
      }
      if (card.review_status && card.review_status !== 'pending') {
        var rvColor = card.review_status === 'accept' || card.review_status === 'accepted' ? 'var(--success)' : 'var(--danger)';
        var rvIcon = card.review_status === 'accept' || card.review_status === 'accepted' ? '✅' : '❌';
        html += '<div style="font-size:10px;color:' + rvColor + ';margin-top:2px">' + rvIcon + ' review: ' + escHtml(card.review_status) + '</div>';
      }
      html += '<div style="color:' + priColor + ';font-size:10px;margin-top:2px">P' + (card.priority || 5) + '</div>';
      if (card.delegation_result) {
        var shortResult = (card.delegation_result || '').substring(0, 80);
        html += '<div style="font-size:10px;color:var(--text3);margin-top:4px;padding:3px;background:var(--surface);border-radius:3px;max-height:40px;overflow:hidden">' + escHtml(shortResult) + '</div>';
      }
      html += '</div>';
    });
    if (colCards.length === 0) html += '<div style="text-align:center;color:var(--text3);font-size:11px;padding:12px">—</div>';
    html += '</div></div>';
    return html;
  }).join("");
}

function showInlineCardDetail(boardId, cardId) {
  var board = _inlineKanbanBoards.find(function(b) { return b.id === boardId; });
  if (!board) return;
  var card = (board.cards || []).find(function(c) { return c.id === cardId; });
  if (!card) return;
  var html = '<div style="padding:16px">';
  html += '<h3 style="margin:0 0 8px 0">' + escHtml(card.title || '') + '</h3>';
  if (card.description) html += '<div style="margin-bottom:8px;color:var(--text2)">' + escHtml(card.description) + '</div>';
  html += '<div style="display:flex;gap:12px;margin-bottom:8px;font-size:12px;color:var(--text3)">';
  html += '<span>📊 Oszlop: ' + escHtml(card.column || '') + '</span>';
  html += '<span>👤 ' + escHtml(card.assigned_to || '—') + '</span>';
  html += '<span>P' + (card.priority || 5) + '</span>';
  if (card.delegation_task_id) html += '<span>🔗 ' + card.delegation_task_id.substring(0,12) + '</span>';
  html += '</div>';
  if (card.delegation_result) {
    html += '<div style="margin-top:12px;padding:10px;background:var(--surface);border-radius:6px;font-size:12px"><b>Eredmény:</b><br>' + escHtml(card.delegation_result) + '</div>';
  }
  if (card.review_analysis) {
    html += '<div style="margin-top:8px;padding:8px;background:var(--surface);border-radius:6px;font-size:11px;color:var(--text3)"><b>Elemzés:</b> ' + (typeof card.review_analysis === 'object' ? JSON.stringify(card.review_analysis) : escHtml(card.review_analysis)) + '</div>';
  }
  if (card.review_status && card.review_status !== 'pending') {
    var rvColor2 = card.review_status === 'accept' || card.review_status === 'accepted' ? 'var(--success)' : 'var(--danger)';
    var rvIcon2 = card.review_status === 'accept' || card.review_status === 'accepted' ? '✅' : '❌';
    html += '<div style="margin-top:8px;padding:8px;background:var(--surface);border-radius:6px;font-size:12px"><b style="color:' + rvColor2 + '">' + rvIcon2 + ' Review: ' + escHtml(card.review_status) + '</b>';
    if (card.review_reason) html += '<div style="margin-top:4px;color:var(--text3);font-size:11px">' + escHtml(card.review_reason) + '</div>';
    html += '</div>';
  }
  if (card.agent_history && card.agent_history.length > 0) {
    html += '<div style="margin-top:12px"><b style="font-size:13px">🤝 Agent történet:</b></div>';
    html += '<div style="margin-top:6px;display:flex;flex-direction:column;gap:6px">';
    card.agent_history.forEach(function(h) {
      var icon = h.role === 'delegator' ? '📤' : h.role === 'executor' ? '⚙️' : h.role === 'reviewer' ? '🔍' : '👤';
      var bg = h.role === 'reviewer' ? 'rgba(255,193,7,0.1)' : h.role === 'executor' ? 'rgba(76,175,80,0.1)' : 'rgba(33,150,243,0.1)';
      var vc = h.verdict === 'accept' ? 'var(--success)' : h.verdict === 'reject' ? 'var(--danger)' : 'var(--text3)';
      html += '<div style="padding:8px;background:' + bg + ';border-radius:6px;font-size:12px;border-left:3px solid ' + vc + '">';
      html += '<div><b>' + icon + ' ' + escHtml(h.agent || '') + '</b> <span style="color:var(--text3);font-size:10px">(' + escHtml(h.role || '') + ')</span></div>';
      html += '<div style="color:var(--text3);font-size:11px;margin-top:2px">' + escHtml(h.action || '') + '</div>';
      if (h.verdict) html += '<div style="margin-top:2px"><b style="color:' + vc + '">Verdict: ' + escHtml(h.verdict) + '</b></div>';
      if (h.reason) html += '<div style="color:var(--text3);font-size:11px;margin-top:2px">' + escHtml(h.reason) + '</div>';
      if (h.result) html += '<div style="color:var(--text3);font-size:10px;margin-top:2px;max-height:60px;overflow:hidden">' + escHtml(h.result.substring(0,200)) + '</div>';
      html += '</div>';
    });
    html += '</div>';
  }
  if (card.approval_required) {
    html += '<button class="btn btn-sm" style="background:var(--success);color:#fff;margin-top:8px" onclick="approveInlineCard(\'' + boardId + '\',\'' + cardId + '\')">✅ Jóváhagy</button>';
  }
  html += '</div>';
  // Show in a simple alert-like overlay
  var overlay = document.createElement('div');
  overlay.style.cssText = 'position:fixed;top:0;left:0;width:100%;height:100%;background:rgba(0,0,0,.7);display:flex;align-items:center;justify-content:center;z-index:300';
  overlay.onclick = function() { document.body.removeChild(overlay); };
  var box = document.createElement('div');
  box.style.cssText = 'background:var(--surface);border-radius:12px;max-width:500px;width:90%;max-height:80vh;overflow-y:auto;border:1px solid var(--border)';
  box.onclick = function(e) { e.stopPropagation(); };
  box.innerHTML = '<div style="display:flex;justify-content:space-between;align-items:center;padding:12px 16px;border-bottom:1px solid var(--border)"><b>Kártya</b><button style="background:none;border:none;color:var(--text);font-size:20px;cursor:pointer" onclick="document.body.removeChild(document.getElementById(\'cardDetailOverlay\'))">✕</button></div>' + html;
  overlay.id = 'cardDetailOverlay';
  overlay.appendChild(box);
  document.body.appendChild(overlay);
}

function approveInlineCard(boardId, cardId) {
  var token = localStorage.getItem("a2a_token") || localStorage.getItem("mesh_token") || "";
  fetch("/api/kanban/" + boardId + "/cards/" + cardId + "/approve", {
    method: "POST",
    headers: {"Content-Type": "application/json", "Authorization": "Bearer " + token}
  }).then(function(r) { return r.json(); }).then(function(d) {
    if (d.status === "approved") {
      var overlay = document.getElementById('cardDetailOverlay');
      if (overlay) document.body.removeChild(overlay);
      loadInlineKanban();
    } else {
      alert("Hiba: " + (d.error || "ismeretlen"));
    }
  });
}

function showKanbanCardDetail(cardId) {
  var token = localStorage.getItem("a2a_token") || localStorage.getItem("mesh_token") || "";
  // Hide delegations modal background
  var delModal = document.getElementById('delegationsModal');
  if (delModal) delModal.style.display = 'none';
  fetch("/api/kanban/cards/" + cardId, {headers: {"Authorization": "Bearer " + token}})
    .then(r => r.json())
    .then(card => {
      // If card has delegation_task_id, redirect to full task detail
      if (card.delegation_task_id) {
        showTaskDetail(card.delegation_task_id);
        return;
      }
      
      var modal = document.createElement('div');
      modal.className = 'modal-overlay';
      modal.id = 'kanbanCardDetailModal';
      modal.style.zIndex = '1000';
      modal.style.display = 'flex';
      modal.onclick = function(e) { if (e.target === modal) modal.remove(); };
      
      var approveBtn = card.approval_required ? '<button class="btn btn-sm" style="background:var(--success)" onclick="approveKanbanCard(\'' + card.id + '\')">✅ Jóváhagyás</button>' : '';
      
      // Delegation result section
      var resultHtml = '';
      if (card.delegation_result) {
        resultHtml = '<div style="margin-bottom:12px"><h4 style="margin:0 0 8px 0;font-size:13px">📄 Eredmény</h4>' +
          '<div style="padding:10px;background:var(--surface2);border-radius:8px;font-size:12px;color:var(--text2);max-height:200px;overflow-y:auto;white-space:pre-wrap;line-height:1.4">' + escHtml(card.delegation_result).substring(0, 3000) + '</div></div>';
      }
      
      // Source info
      var sourceHtml = card.source ? '<div><b style="color:var(--text3)">Forrás:</b> ' + escHtml(card.source) + '</div>' : '';
      var updatedHtml = card.updated_at ? '<div><b style="color:var(--text3)">Módosítva:</b> ' + new Date(card.updated_at).toLocaleString() + '</div>' : '';
      
      // Status badge
      var colLabels = {todo:"📋 Teendő", in_progress:"⚙️ Folyamatban", review:"👁️ Ellenőrzés", done:"✅ Kész"};
      var colColors = {todo:"var(--text3)", in_progress:"var(--warning)", review:"#8b5cf6", done:"var(--success)"};
      var statusBadge = '<span style="background:' + (colColors[card.column]||'var(--info)') + ';color:#fff;padding:2px 8px;border-radius:4px;font-size:11px">' + (colLabels[card.column]||card.column) + '</span>';
      
      modal.innerHTML = '<div class="file-modal" style="width:600px;max-width:95vw;max-height:80vh;overflow-y:auto">' +
        '<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px">' +
        '<h2 style="margin:0">📋 ' + escHtml(card.title) + '</h2>' +
        '<button style="background:none;border:none;color:var(--text);font-size:24px;cursor:pointer" onclick="document.getElementById(\'kanbanCardDetailModal\').remove();var dm=document.getElementById(\'delegationsModal\');if(dm)dm.style.display=\'flex\';">✕</button>' +
        '</div>' +
        '<div style="margin-bottom:12px;font-size:14px;line-height:1.6">' +
        '<div style="margin-bottom:8px">' + statusBadge + ' <span class="priority-badge priority-' + (card.priority||5) + '">P' + (card.priority||5) + '</span></div>' +
        '<div><b style="color:var(--text3)">Létrehozva:</b> ' + (card.created_at ? new Date(card.created_at).toLocaleString() : '...') + '</div>' +
        updatedHtml + sourceHtml +
        (card.assigned_to ? '<div><b style="color:var(--text3)">Felelős:</b> 👤' + escHtml(card.assigned_to) + '</div>' : '') +
        '</div>' +
        '<div style="margin-bottom:12px;padding:10px;background:var(--surface2);border-radius:8px;font-size:13px;color:var(--text2);line-height:1.5">' + escHtml(card.description || 'Nincs leírás') + '</div>' +
        resultHtml +
        '<div style="display:flex;justify-content:flex-end;gap:8px">' +
        approveBtn +
        '<button class="btn btn-sm" style="background:var(--surface2);color:var(--text)" onclick="document.getElementById(\'kanbanCardDetailModal\').remove();var dm=document.getElementById(\'delegationsModal\');if(dm)dm.style.display=\'flex\';">Bezárás</button>' +
        '</div></div>';
      
      document.body.appendChild(modal);
    })
    .catch(function(e) {
      alert('Hiba: ' + e.message);
    });
}

function addCardToBoard() {
  var title = prompt("Kártya címe:");
  if (!title) return;
  var col = prompt("Oszlop (todo, in_progress, review, done):", "todo");
  if (!col) return;
  
  var token = localStorage.getItem("a2a_token") || localStorage.getItem("mesh_token") || "";
  fetch("/api/kanban/1/cards", {
    method: "POST",
    headers: {"Content-Type": "application/json", "Authorization": "Bearer " + token},
    body: JSON.stringify({ title: title, column: col })
  }).then(r => r.json()).then(d => {
    if (d.error) alert("Hiba: " + d.error);
    else loadDelegations();
  });
}

function approveKanbanCard(cardId) {
  var token = localStorage.getItem("a2a_token") || localStorage.getItem("mesh_token") || "";
  fetch("/api/kanban/cards/" + cardId + "/approve", {
    method: "POST",
    headers: {"Authorization": "Bearer " + token}
  }).then(r => r.json()).then(d => {
    if (d.error) alert("Hiba: " + d.error);
    else {
      var m = document.getElementById('kanbanCardDetailModal');
      if (m) m.remove();
      loadDelegations();
    }
  });
}

// ─── Reflections (Esmefuttatasok) ──────────────────────────

var _reflAllData = [];
var _reflActiveFilter = "";

function showReflections() {
  document.getElementById("reflectionsModal").style.display = "flex";
  _reflActiveFilter = "";
  loadReflections();
}

function loadReflections() {
  var url = "/api/reflections?limit=100";
  if (_reflActiveFilter) url += "&type=" + encodeURIComponent(_reflActiveFilter);
  fetch(url, {headers: {"Authorization": "Bearer " + (localStorage.getItem("mesh_token") || "")}})
    .then(function(r) { return r.json(); })
    .then(function(d) {
      if (d.error) {
        document.getElementById("reflectionsContent").innerHTML = '<div style="color:var(--danger);padding:20px">Hiba: ' + escapeHtml(d.error) + '</div>';
        return;
      }
      _reflAllData = d.reflections || [];
      renderReflections();
    })
    .catch(function() {
      document.getElementById("reflectionsContent").innerHTML = '<div style="color:var(--danger);padding:20px">Hiba a betöltéskor</div>';
    });
}

function renderReflections() {
  var data = _reflAllData;
  var el = document.getElementById("reflectionsContent");
  var statsEl = document.getElementById("reflStats");
  var filtersEl = document.getElementById("reflFilters");

  // Stats
  var typeCounts = {};
  data.forEach(function(r) {
    (r.types || []).forEach(function(t) { typeCounts[t] = (typeCounts[t] || 0) + 1; });
  });
  var statsHtml = '<span>📊 Összes: <b>' + data.length + '</b></span>';
  Object.keys(typeCounts).forEach(function(t) {
    var emoji = {"stagnation":"🔄","consensus":"🤝","blind_spot":"🔍","tension":"⚡","progress":"📈"}[t] || "📊";
    statsHtml += '<span>' + emoji + ' ' + t + ': <b>' + typeCounts[t] + '</b></span>';
  });
  statsEl.innerHTML = statsHtml;

  // Filter buttons
  var filterHtml = '<button class="btn btn-sm" style="' + (!_reflActiveFilter ? "background:var(--primary)" : "") + '" onclick="_reflActiveFilter=\'\';loadReflections()">Mind</button>';
  ["stagnation","consensus","blind_spot","tension","progress"].forEach(function(t) {
    var emoji = {"stagnation":"🔄","consensus":"🤝","blind_spot":"🔍","tension":"⚡","progress":"📈"}[t] || "📊";
    filterHtml += '<button class="btn btn-sm" style="' + (_reflActiveFilter === t ? "background:var(--primary)" : "") + '" onclick="_reflActiveFilter=\'' + t + '\';loadReflections()">' + emoji + ' ' + t + '</button>';
  });
  filtersEl.innerHTML = filterHtml;

  // Content
  if (data.length === 0) {
    el.innerHTML = '<div style="text-align:center;color:var(--text3);padding:40px">Nincsenek eszmefuttatások</div>';
    return;
  }

  el.innerHTML = data.map(function(r) {
    var typeBadges = (r.types || []).map(function(t) {
      var emoji = {"stagnation":"🔄","consensus":"🤝","blind_spot":"🔍","tension":"⚡","progress":"📈"}[t] || "📊";
      var color = {"stagnation":"var(--warning)","consensus":"var(--success)","blind_spot":"var(--info)","tension":"var(--danger)","progress":"var(--primary)"}[t] || "var(--text3)";
      return '<span style="background:' + color + '20;color:' + color + ';padding:2px 8px;border-radius:4px;font-size:11px;margin-right:4px">' + emoji + ' ' + t + '</span>';
    }).join("");

    var agents = (r.agents || []).map(function(a) { return '<span style="color:var(--text3);font-size:11px">@' + escapeHtml(a) + '</span>'; }).join(" ");
    var time = r.created_at ? new Date(r.created_at).toLocaleString("hu-HU") : "";

    return '<div style="border:1px solid var(--border);border-radius:8px;padding:12px;margin-bottom:8px;background:var(--surface2)">' +
      '<div style="display:flex;justify-content:space-between;align-items:start;margin-bottom:6px">' +
        '<div>' + typeBadges + '</div>' +
        '<span style="font-size:11px;color:var(--text3)">' + time + '</span>' +
      '</div>' +
      '<div style="font-size:13px;color:var(--text2);margin-bottom:4px">📌 ' + escapeHtml(r.topic || 'ismeretlen') + '</div>' +
      '<div style="font-size:13px;line-height:1.5;color:var(--text)">' + escapeHtml(r.analysis || '') + '</div>' +
      (agents ? '<div style="margin-top:6px">' + agents + '</div>' : '') +
    '</div>';
  }).join("");
}

function exportReflections() {
  window.open("/api/reflections/export?token=" + encodeURIComponent(localStorage.getItem("mesh_token") || ""), "_blank");
}

window.refreshSharedContext = function() {
  var prefix = document.getElementById('ctxPrefix') ? document.getElementById('ctxPrefix').value : '';
  loadMarveenPage('shared-context', 'prefix=' + encodeURIComponent(prefix));
};

window.showSharedContextModal = function(entry) {
  var isEdit = !!entry;
  var key = isEdit ? entry.key : prompt('Kulcs:');
  if (!key) return;
  
  var value = isEdit ? prompt('Érték:', entry.value) : prompt('Érték:');
  if (value === null) return;
  
  var type = isEdit ? prompt('Típus (text/json):', entry.value_type || 'text') : prompt('Típus (text/json):', 'text');
  if (!type) type = 'text';
  
  var expires = isEdit ? prompt('Lejárás (perc, 0=soha):', entry.expires || '0') : prompt('Lejárás (perc, 0=soha):', '0');
  if (!expires) expires = '0';

  var token = localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || '';
  fetch('/api/context', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + token },
    body: JSON.stringify({ key: key, value: value, value_type: type, expires: parseInt(expires, 10) })
  })
  .then(function(r) { return r.json(); })
  .then(function(d) { 
    if (d.error) { alert('Hiba: ' + d.error); } 
    else { alert('Mentve'); loadMarveenPage('shared-context'); } 
  })
  .catch(function(e) { alert('Hiba: ' + e.message); });
};

window.deleteSharedContext = function(key) {
  if (!key || !confirm('Biztosan törlöd a ' + key + ' bejegyzést?')) return;
  var token = localStorage.getItem('a2a_token') || localStorage.getItem('mesh_token') || '';
  fetch('/api/context/' + encodeURIComponent(key), { method: 'DELETE', headers: { 'Authorization': 'Bearer ' + token } })
  .then(function(r) { return r.json(); })
  .then(function(d) { 
    if (d.error) { alert('Hiba: ' + d.error); } 
    else { alert('Törölve'); loadMarveenPage('shared-context'); } 
  })
  .catch(function(e) { alert('Hiba: ' + e.message); });
};