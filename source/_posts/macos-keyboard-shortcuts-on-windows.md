---
title: 將 MacOS 上的鍵盤快速鍵設定到 Windows 
date: 2024-08-15
tags: [windows, macos, keyboard, shortcuts]
---

## 緣起

因為已經使用十年以上的 macOS，已經非常習慣 macOS 的鍵盤快速鍵導致常常在電玩機 Windows 上按錯，因此興起了想要把 macOS 的快速鍵移植到 Windows 上

## 方法

大原則是: 
1. 開發者或方法盡可能公開透明
2. 盡可能不要動到底層核心
3. 可還原 (雖然重灌治百病)

因此選到了 Windows 本家的 [PowerToys](https://learn.microsoft.com/zh-tw/windows/powertoys/) ([如何安裝](https://learn.microsoft.com/zh-tw/windows/powertoys/install))

```powershell
winget install --id Microsoft.PowerToys --source winget
```

快速掃過去 PowerToys 提供的功能，其中有幾個也跟 macOS 提供的很像，例如:
- 色彩選擇器 (ColorPicker)
- Always On Top
- 預覽

以及今天的主角: [鍵盤管理器 (Keyboard Manager)](https://learn.microsoft.com/zh-tw/windows/powertoys/keyboard-manager)

開始之前要先知道他有甚麼限制

> 有一些快捷鍵是由操作系統保留或無法替換的。 無法重新對應的按鍵包括：
> ⊞ Win+L and Ctrl+Alt+Del 無法被重新對應，因為它們是由 Windows OS 保留的。
> Fn (函式) 鍵無法重新對應 (在大多數情況下)。 The F1 ~ F12 (和 F13 ~ F24) 鍵可以對應。
> 暫停 只會傳送單一按鍵關閉事件。 例如，將其對應到退格鍵，按住後只會刪除單一字元。
> 即使重新指派，⊞ Win+G 通常也會開啟 Xbox 遊戲列。 遊戲列可以在 Windows[設定] 中停用。

和他的[常見問題集](https://learn.microsoft.com/zh-tw/windows/powertoys/keyboard-manager#frequently-asked-questions)以及[已知問題](https://learn.microsoft.com/zh-tw/windows/powertoys/keyboard-manager#frequently-asked-questions)

另外當你開啟設定畫面的時候，所以鍵盤設定會失效 (如下圖)

![setup](/images/macos-keyboard-shortcuts-on-windows/screenshot-setup.png)

最後直接附上幾個畫面和設定檔

下圖是關閉不需要的功能

![dashbord](/images/macos-keyboard-shortcuts-on-windows/screenshot-dashboard.png)

下圖是表示設定了哪些鍵盤對應和快速鍵

![mappings](/images/macos-keyboard-shortcuts-on-windows/screenshot-mappings.png)

以及最後的是在 `C:\Users\${username}\AppData\Local\Microsoft\PowerToys\Keyboard Manager` 底下的設定檔 `default.json`

```json
{"remapKeys":{"inProcess":[{"originalKeys":"163","newRemapKeys":"260"},{"originalKeys":"91","newRemapKeys":"17"},{"originalKeys":"20","newRemapKeys":"16"},{"originalKeys":"92","newRemapKeys":"17"},{"originalKeys":"162","newRemapKeys":"260"}]},"remapKeysToText":{"inProcess":[]},"remapShortcuts":{"global":[{"originalKeys":"18;37","operationType":0,"secondKeyOfChord":0,"newRemapKeys":"17;37"},{"originalKeys":"18;39","operationType":0,"secondKeyOfChord":0,"newRemapKeys":"17;39"},{"originalKeys":"17;9","operationType":0,"secondKeyOfChord":0,"newRemapKeys":"18;9"},{"originalKeys":"17;32","operationType":0,"secondKeyOfChord":0,"newRemapKeys":"260;32"},{"originalKeys":"17;37","newRemapKeys":"36"},{"originalKeys":"17;39","newRemapKeys":"35"}],"appSpecific":[]},"remapShortcutsToText":{"global":[],"appSpecific":[]}}
```

取用 `default.json` 的時候請記得要先備份原本的
