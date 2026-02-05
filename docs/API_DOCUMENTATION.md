# Mapping ID 追蹤 API 文件

---

## 端點 (Endpoint)

| 項目      | 詳情                                            |
| :------ | :-------------------------------------------- |
| **URL** | `https://pixel.omnisegment.com/track`         |
| **方法**  | `GET`                                         |
| **用途**  | 用於產生用戶 ID 對映 (User ID Mapping) 並寫入瀏覽器 cookie，同時記錄追蹤數據。 |
| **請求頻率**  | 每次頁面載入時觸發一次（相當於 PageView）。 |

---

## 請求參數 (Query Parameters)

| 參數        | 必填 | 說明                                  | 範例值          |
| :-------- | :--- | :---------------------------------- | :----------- |
| `cid`     | 必填* | 客戶/用戶 ID (Client ID)，系統中唯一識別用戶的 ID。 | `user_12345` |
| `to`      | 選填 | 重定向 URL，設置 cookie 後將用戶重定向到此 URL。 | `https://onead.onevision.com.tw/v2/pixel/os` |



### 重定向 URL 限制

`to` 參數目前僅接受以下允許的白名單域名：
- `onead.onevision.com.tw`

---

## 使用範例 (Usage Examples)

### 1. 基本追蹤

```javascript
const cid = 'user_12345';
const partner = 'SpecialPartner';
const url = `https://pixel.omnisegment.com/track?cid=${cid}&partner=${partner}`;

const img = new Image();
img.src = url;
```

#### Response
- **HTTP 狀態碼**: 200 OK
- **Content-Type**: `image/gif`
- **Body**: 1x1 透明 GIF 二進制數據

### 2. 追蹤並重定向

```javascript
const cid = 'user_12345';
const redirectUrl = encodeURIComponent('https://onead.onevision.com.tw/v2/pixel/os');
const url = `https://pixel.omnisegment.com/track?cid=${cid}&to=${redirectUrl}`;

const img = new Image();
img.src = url;
```


#### Response
- **HTTP 狀態碼**: 302 Found
- **Location**: 重定向 URL，並在 query string 中自動添加 `id={mapping_id}` 參數


**to 參數處理**：
- 系統會**保留** URL 中的所有原有參數
- 如果目標 URL 已包含 `id` 參數，將被 `{mapping_id}` 覆蓋

**範例**：

 **基本重定向（無額外參數）**：
```
請求: https://pixel.omnisegment.com/track?cid=user123&to=https://onead.onevision.com.tw/v2/pixel/os

重定向到: https://onead.onevision.com.tw/v2/pixel/os?id={mapping_id}
```

**支援額外參數**：目標 URL 可以包含多個參數，會保留所有參數並覆蓋 `id` 參數。
```
請求:
https://pixel.omnisegment.com/track?cid=user123&to=https://onead.onevision.com.tw/v2/pixel/os?id=bebit666&campaign=summer&source=email

重定向到: https://onead.onevision.com.tw/v2/pixel/os?id={mapping_id}&campaign=summer&source=email
```
*其他參數（`campaign`、`source`）會被保留，只有 `id` 參數會被覆蓋*

### 3. 使用 fetch 發送請求（包含 credentials）

```javascript
const cid = 'user_12345';
const url = `https://pixel.omnisegment.com/track?cid=${cid}`;

fetch(url, {
  credentials: "include",
  mode: "no-cors"
})
```

**說明**：
- `credentials: "include"` - 確保請求會發送和接收 cookies（跨域請求時也包含）
- `mode: "no-cors"` - 允許跨域請求，適用於追蹤像素場景

#### 搭配重定向使用

```javascript
const cid = 'user_12345';
const redirectUrl = encodeURIComponent('https://onead.onevision.com.tw/v2/pixel/os');
const url = `https://pixel.omnisegment.com/track?cid=${cid}&to=${redirectUrl}`;

fetch(url, {
  credentials: "include",
  mode: "no-cors"
})
```

---


## 錯誤處理

所有錯誤情況下，API 都會返回追蹤像素（200 OK），不會返回錯誤狀態碼，以確保追蹤請求不影響頁面載入。

### 常見情況

| 情況 | 行為 |
| :--- | :--- |
| 缺少 `cid` 參數 | 返回追蹤像素，但不記錄數據 |
| `to` 參數域名不在白名單 | 忽略重定向，返回追蹤像素 |
| `to` 參數格式無效 | 忽略重定向，返回追蹤像素 |

---
