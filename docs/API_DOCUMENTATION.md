# Mapping ID 追蹤 API 文件

---

## 端點 (Endpoints)

###  追蹤端點 (Tracking Endpoint)

| 項目      | 詳情                                            |
| :------ | :-------------------------------------------- |
| **URL** | `https://pixel.omnisegment.com/track`         |
| **方法**  | `GET`                                         |
| **用途**  | 用於產生用戶 ID 對映 (User ID Mapping) 並寫入瀏覽器 cookie，同時記錄追蹤數據。 |
| **請求頻率**  | 每次頁面載入時觸發一次（相當於 PageView）。 |


## 請求參數 (Query Parameters)

| 參數        | 必填 | 說明                                  | 範例值          |
| :-------- | :--- | :---------------------------------- | :----------- |
| `cid`     | 必填* | 客戶/用戶 ID (Client ID)，系統中唯一識別用戶的 ID。 | `user_12345` |
| `to`      | 選填 | 重定向 URL，設置 cookie 後將用戶重定向到此 URL。 | `https://onead.onevision.com.tw/v2/pixel/os` |
| `partner` | 選填 | 合作夥伴識別碼，用於標記流量來源。如未提供，系統會根據 `to` 參數的域名自動識別。 | `OneAD` |



### Partner 自動識別

系統會根據以下優先順序決定合作夥伴標記：
1. **`partner` 參數**：如果請求中包含 `partner` 參數，將優先使用。
2. **域名映射**：如果未提供 `partner` 參數，系統會根據 `to` 參數的域名自動識別合作夥伴。
3. **預設值**：如果以上都無法識別，將使用 `"unknown"` 作為合作夥伴標記。

**域名與合作夥伴對應表**：

| 域名                         | 合作夥伴標記 |
| :-------------------------- | :------- |
| `onead.onevision.com.tw`    | `OneAD`  |
| `localhost`                 | `test`   |

### 重定向 URL 限制

`to` 參數目前僅接受以下允許的白名單域名：
- `onead.onevision.com.tw`
- `localhost` (僅用於開發和測試)

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


### 3. 使用 fetch 發送請求（包含 credentials）

```javascript
const cid = 'user_12345';

const url = `https://pixel.omnisegment.com/track?cid=${cid}&partner=${partner}`;

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
const partner = 'SpecialPartner';
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
