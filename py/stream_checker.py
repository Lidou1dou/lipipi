#!/usr/bin/env python3
import asyncio
import aiohttp
import json
import re
import os
import time
import glob
from urllib.parse import urlparse, urljoin
import logging
from tqdm import tqdm
import pickle

# 尝试导入繁体转简体的库
try:
    from opencc import OpenCC
    cc = OpenCC('t2s')  # 繁体转简体
    HAS_OPENCC = True
except ImportError:
    HAS_OPENCC = False
    print("提示: 未安装opencc-python-reimplemented库，将不会进行繁简转换")
    print("可以通过运行 pip install opencc-python-reimplemented 来安装")

# =====================================================================
# 配置部分 - 直接修改这里的变量来配置程序
# =====================================================================

# 要扫描的目录路径，使用绝对路径或相对路径
DIRECTORY = "."

# 要包含的文件扩展名列表
EXTENSIONS = [".txt", ".m3u", ".m3u8"]

# 最大并发请求数
CONCURRENCY = 200

# 请求超时时间（秒）
TIMEOUT = 10

# 是否递归扫描子目录
RECURSIVE = True

# 结果保存文件路径
OUTPUT_FILE = f"results_{time.strftime('%Y%m%d_%H%M%S')}.json"

# 自定义 User-Agent
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"

# 断点续传文件路径
CHECKPOINT_FILE = "stream_checker_checkpoint.pkl"

# 是否启用断点续传
ENABLE_CHECKPOINT = True

# 每处理多少个URL保存一次结果
SAVE_INTERVAL = 100

# 是否将繁体字转换为简体字
CONVERT_TO_SIMPLIFIED = True

# 是否只保留一个最终输出文件
KEEP_ONLY_FINAL_FILE = True

# 是否在最终文件中包含聚合分组部分
INCLUDE_AGGREGATED_SECTION = False

# 是否对m3u8文件进行深度验证
DEEP_VALIDATION = True

# 验证m3u8文件时检查的分片数量
M3U8_SEGMENTS_TO_CHECK = 3

# 是否验证m3u8文件的内容格式
VALIDATE_M3U8_CONTENT = True

# 是否验证其他格式的流媒体
VALIDATE_OTHER_STREAMS = True

# 是否使用ffprobe进行高级验证（需要安装ffmpeg）
USE_FFPROBE = False

# =====================================================================
# 以下是程序代码，一般情况下不需要修改
# =====================================================================

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("stream_checker.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Regular expression to match URLs
URL_PATTERN = re.compile(r'https?://[^\s"\'<>]+')

class StreamChecker:
    def __init__(self, concurrency=100, timeout=10, user_agent=None, save_interval=100, checkpoint_file=None):
        """
        Initialize the stream checker.
        
        Args:
            concurrency (int): Maximum number of concurrent requests
            timeout (int): Timeout for each request in seconds
            user_agent (str): User agent string for HTTP requests
            save_interval (int): Save results after processing this many URLs
            checkpoint_file (str): Path to save checkpoint data
        """
        self.concurrency = concurrency
        self.timeout = timeout
        self.user_agent = user_agent or "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        self.semaphore = asyncio.Semaphore(concurrency)
        self.results = {
            "valid": [],
            "invalid": [],
            "timeout": [],
            "error": []
        }
        self.total_urls = 0
        self.processed_urls = 0
        self.progress_bar = None
        self.save_interval = save_interval
        self.checkpoint_file = checkpoint_file
        self.processed_url_hashes = set()  # 存储已处理URL的哈希值
        self.output_file = None
        self.last_save_count = 0
        # 常见的直播流内容类型
        self.stream_content_types = [
            'application/vnd.apple.mpegurl',
            'application/x-mpegurl',
            'application/octet-stream',
            'video/mp2t',
            'video/mp4',
            'video/x-flv',
            'audio/mpegurl',
            'audio/x-mpegurl',
            'application/dash+xml',
            'video/x-ms-asf',
            'video/x-ms-wmv',
            'video/webm',
            'application/x-rtsp',
            'application/x-rtmp',
            'application/x-shockwave-flash',
            'flv-application/octet-stream',
            'video/x-matroska'
        ]
        
        # 常见的直播流URL模式
        self.stream_url_patterns = [
            r'\.m3u8(\?.*)?$',
            r'\.flv(\?.*)?$',
            r'\.ts(\?.*)?$',
            r'\.mp4(\?.*)?$',
            r'\.mpd(\?.*)?$',
            r'\.ism(\?.*)?$',
            r'\.smil(\?.*)?$',
            r'\.f4m(\?.*)?$',
            r'rtmp://',
            r'rtsp://',
            r'mms://',
            r'mmsh://',
            r'\.php\?.*id=',
            r'\.php\?.*key=',
            r'\.php\?.*token=',
            r'\.php\?.*stream=',
            r'\.php\?.*channel=',
            r'\.php\?.*live=',
            r'\.php\?.*url=',
            r'/live/',
            r'/play/',
            r'/stream/',
            r'/hls/',
            r'/dash/',
            r'/api/live',
            r'/api/stream'
        ]

    async def check_url(self, url_info):
        """
        Check if a URL is a valid streaming source.
        
        Args:
            url_info (dict): Dictionary containing URL and name information
            
        Returns:
            tuple: (url_info, status, response_time, error_message)
        """
        url = url_info["url"]
        
        async with self.semaphore:
            start_time = time.time()
            error_message = None
            status = "invalid"
            
            try:
                # Parse URL to ensure it's valid
                parsed_url = urlparse(url)
                if not parsed_url.scheme or not parsed_url.netloc:
                    return url_info, "invalid", 0, "Invalid URL format"
                
                # 检查URL是否匹配已知的流媒体模式
                is_likely_stream = False
                for pattern in self.stream_url_patterns:
                    if re.search(pattern, url, re.IGNORECASE):
                        is_likely_stream = True
                        break
                
                timeout = aiohttp.ClientTimeout(total=self.timeout)
                headers = {
                    "User-Agent": self.user_agent,
                    "Accept": "*/*",
                    "Connection": "keep-alive",
                    "Referer": f"{parsed_url.scheme}://{parsed_url.netloc}/",
                    "Origin": f"{parsed_url.scheme}://{parsed_url.netloc}",
                    "X-Requested-With": "XMLHttpRequest"
                }
                
                # 对于RTMP链接，直接尝试使用ffprobe验证（如果启用）
                if url.lower().startswith('rtmp://') and USE_FFPROBE:
                    is_valid, error = await self._validate_with_ffprobe(url)
                    if is_valid:
                        status = "valid"
                    else:
                        error_message = error
                    return url_info, status, time.time() - start_time, error_message
                
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    try:
                        # 对于直播流，我们需要检查内容而不仅仅是头信息
                        async with session.get(url, headers=headers, allow_redirects=True) as response:
                            if response.status < 400:
                                # 检查内容类型
                                content_type = response.headers.get('Content-Type', '').lower()
                                
                                # 如果是m3u8文件并且启用了深度验证，进行更深入的检查
                                if (url.lower().endswith('.m3u8') or 'mpegurl' in content_type) and DEEP_VALIDATION:
                                    content = await response.text()
                                    validation_result = await self._validate_m3u8(url, content, session, headers)
                                    if validation_result[0]:
                                        status = "valid"
                                    else:
                                        status = "invalid"
                                        error_message = validation_result[1]
                                # 如果是FLV文件并且启用了验证，进行检查
                                elif (url.lower().endswith('.flv') or 'flv' in content_type) and VALIDATE_OTHER_STREAMS:
                                    content_preview = await response.content.read(12)  # FLV头部是9字节
                                    if self._validate_flv(content_preview):
                                        status = "valid"
                                    else:
                                        status = "invalid"
                                        error_message = "Not a valid FLV stream"
                                # 检查内容类型是否为已知的流媒体类型
                                elif any(stream_type in content_type for stream_type in self.stream_content_types):
                                    status = "valid"
                                # 检查响应内容的前几个字节，看是否符合流媒体格式
                                elif VALIDATE_OTHER_STREAMS:
                                    # 读取前1024字节进行格式检查
                                    content_preview = await response.content.read(1024)
                                    if self._check_stream_content(content_preview, url):
                                        status = "valid"
                                    else:
                                        # 如果URL模式匹配已知的流媒体模式，我们可能仍然认为它是有效的
                                        if is_likely_stream:
                                            status = "valid"
                                        else:
                                            status = "invalid"
                                            error_message = "Content does not appear to be a valid stream"
                                else:
                                    # 如果没有启用验证，但URL模式匹配已知的流媒体模式，我们认为它是有效的
                                    if is_likely_stream:
                                        status = "valid"
                                    else:
                                        status = "invalid"
                                        error_message = "URL does not match known stream patterns"
                            else:
                                error_message = f"HTTP status: {response.status}"
                    except asyncio.TimeoutError:
                        status = "timeout"
                        error_message = "Request timed out"
                    except Exception as e:
                        status = "error"
                        error_message = str(e)
            except Exception as e:
                status = "error"
                error_message = str(e)
            
            response_time = time.time() - start_time
            
            # Update progress
            self.processed_urls += 1
            if self.progress_bar:
                self.progress_bar.update(1)
            
            # 检查是否需要保存中间结果
            if self.processed_urls % self.save_interval == 0 and self.processed_urls > self.last_save_count:
                await self.save_intermediate_results()
                self.last_save_count = self.processed_urls
            
            return url_info, status, response_time, error_message

    def extract_urls_from_file(self, file_path):
        """
        Extract URLs and their names from a file based on its extension.
        
        Args:
            file_path (str): Path to the file
            
        Returns:
            list: List of dictionaries containing URLs and their names
        """
        url_infos = []
        
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
                
            # Handle JSON files
            if file_path.endswith('.json'):
                try:
                    data = json.loads(content)
                    # Extract URLs based on common JSON structures
                    self._extract_urls_from_json(data, url_infos)
                except json.JSONDecodeError:
                    # If not valid JSON, try regex
                    self._extract_urls_from_text(content, url_infos, os.path.basename(file_path))
            # Handle M3U files
            elif file_path.endswith('.m3u') or file_path.endswith('.m3u8'):
                self._extract_urls_from_m3u(content, url_infos)
            else:
                # For text files, try to extract URLs and names
                self._extract_urls_from_text(content, url_infos, os.path.basename(file_path))
                
        except Exception as e:
            logger.error(f"Error processing file {file_path}: {str(e)}")
        
        # 处理URL中的#符号，只保留#符号前面的部分
        for url_info in url_infos:
            if '#' in url_info["url"] and url_info["url"].startswith('http'):
                # 只保留第一个#符号前面的部分
                url_info["url"] = url_info["url"].split('#')[0]
            
            # 将繁体字转换为简体字
            if CONVERT_TO_SIMPLIFIED and HAS_OPENCC:
                if "name" in url_info:
                    url_info["name"] = cc.convert(url_info["name"])
                if "category" in url_info and url_info["category"]:
                    url_info["category"] = cc.convert(url_info["category"])
        
        # Remove duplicates while preserving order
        unique_url_infos = []
        seen_urls = set()
        for url_info in url_infos:
            url = url_info["url"]
            if url not in seen_urls:
                seen_urls.add(url)
                unique_url_infos.append(url_info)
                
        return unique_url_infos
    
    def _extract_urls_from_json(self, data, url_infos):
        """
        Recursively extract URLs from JSON data.
        
        Args:
            data: JSON data (dict, list, or primitive)
            url_infos (list): List to append URL info dictionaries to
        """
        if isinstance(data, dict):
            # Try to find name and URL pairs
            name = None
            url = None
            
            # Look for common name keys
            for name_key in ['name', 'title', 'label', 'channel', 'tvg-name']:
                if name_key in data and isinstance(data[name_key], str):
                    name = data[name_key].strip()
                    break
            
            # Look for URL
            for url_key in ['url', 'link', 'src', 'href', 'api']:
                if url_key in data and isinstance(data[url_key], str) and (data[url_key].startswith('http://') or data[url_key].startswith('https://')):
                    url = data[url_key]
                    # 忽略包含jar的链接
                    if 'jar' in url.lower():
                        continue
                    # 处理URL中的#符号，只保留#符号前面的部分
                    if '#' in url:
                        url = url.split('#')[0]
                    break
            
            # If we found both name and URL, add them
            if name and url:
                url_infos.append({"name": name, "url": url})
            # If we only found URL, add it with a generic name
            elif url:
                url_infos.append({"name": f"Stream {len(url_infos) + 1}", "url": url})
            
            # Check if any value is a URL
            for key, value in data.items():
                if isinstance(value, str) and (value.startswith('http://') or value.startswith('https://')):
                    # 忽略包含jar的链接
                    if 'jar' in value.lower():
                        continue
                    
                    # 处理URL中的#符号，只保留#符号前面的部分
                    url_value = value
                    if '#' in url_value:
                        url_value = url_value.split('#')[0]
                    
                    # If the key looks like a name, use it
                    if isinstance(key, str) and not key.startswith('_') and key not in ['url', 'link', 'src', 'href', 'api', 'ext', 'jar']:
                        url_infos.append({"name": key, "url": url_value})
                    # Otherwise use a generic name
                    else:
                        url_infos.append({"name": f"Stream {len(url_infos) + 1}", "url": url_value})
                
                # Recurse into nested structures
                if isinstance(value, (dict, list)):
                    self._extract_urls_from_json(value, url_infos)
        elif isinstance(data, list):
            for item in data:
                self._extract_urls_from_json(item, url_infos)
    
    def _extract_urls_from_m3u(self, content, url_infos):
        """
        Extract URLs and names from M3U content.
        
        Args:
            content (str): M3U file content
            url_infos (list): List to append URL info dictionaries to
        """
        lines = content.splitlines()
        current_name = None
        
        for i, line in enumerate(lines):
            line = line.strip()
            
            # Skip empty lines
            if not line:
                continue
            
            # Check for EXTINF line which contains the name
            if line.startswith('#EXTINF:'):
                # Extract name from EXTINF line
                name_match = re.search(r'tvg-name="([^"]+)"', line)
                if name_match:
                    current_name = name_match.group(1).strip()
                else:
                    # Try to extract name from the end of the line
                    parts = line.split(',', 1)
                    if len(parts) > 1:
                        current_name = parts[1].strip()
                    else:
                        current_name = f"Stream {len(url_infos) + 1}"
            # Check if line is a URL
            elif line.startswith('http://') or line.startswith('https://'):
                name = current_name or f"Stream {len(url_infos) + 1}"
                
                # 处理URL中的#符号，只保留#符号前面的部分
                url = line
                if '#' in url:
                    url = url.split('#')[0]
                
                url_infos.append({"name": name, "url": url})
                current_name = None  # Reset name for next URL
            # Check for URLs in the line using regex
            else:
                urls = URL_PATTERN.findall(line)
                for url in urls:
                    name = current_name or f"Stream {len(url_infos) + 1}"
                    
                    # 处理URL中的#符号，只保留#符号前面的部分
                    if '#' in url:
                        url = url.split('#')[0]
                    
                    url_infos.append({"name": name, "url": url})
                    current_name = None  # Reset name for next URL
    
    def _extract_urls_from_text(self, content, url_infos, default_source=""):
        """
        Extract URLs and names from text content.
        
        Args:
            content (str): Text file content
            url_infos (list): List to append URL info dictionaries to
            default_source (str): Default source name if no name is found
        """
        lines = content.splitlines()
        current_category = default_source
        
        # Check if the file follows the genre format (name,url)
        for i, line in enumerate(lines):
            line = line.strip()
            
            # Skip empty lines
            if not line:
                continue
            
            # Check for category/genre line
            if line.endswith(',#genre#') or line.endswith('#type#'):
                current_category = line.split(',')[0].strip()
                continue
            
            # Check for name,url format
            parts = line.split(',', 1)
            if len(parts) == 2:
                name = parts[0].strip()
                url_part = parts[1].strip()
                
                # Check if the second part is a URL
                if url_part.startswith('http://') or url_part.startswith('https://'):
                    # 忽略包含jar的链接
                    if 'jar' in url_part.lower():
                        continue
                    
                    # 处理URL中的#符号，只保留#符号前面的部分
                    if '#' in url_part:
                        url_part = url_part.split('#')[0]
                    
                    url_infos.append({"name": name, "url": url_part, "category": current_category})
                else:
                    # Try to find URLs in the second part
                    urls = URL_PATTERN.findall(url_part)
                    for url in urls:
                        # 忽略包含jar的链接
                        if 'jar' in url.lower():
                            continue
                        
                        # 处理URL中的#符号，只保留#符号前面的部分
                        if '#' in url:
                            url = url.split('#')[0]
                        
                        url_infos.append({"name": name, "url": url, "category": current_category})
            else:
                # Try to find URLs in the line
                urls = URL_PATTERN.findall(line)
                for url in urls:
                    # 忽略包含jar的链接
                    if 'jar' in url.lower():
                        continue
                    
                    # 处理URL中的#符号，只保留#符号前面的部分
                    if '#' in url:
                        url = url.split('#')[0]
                    
                    # Use line as name if it's not just the URL
                    if line != url:
                        name = line.replace(url, '').strip()
                        if name:
                            url_infos.append({"name": name, "url": url, "category": current_category})
                        else:
                            url_infos.append({"name": f"{current_category} Stream {len(url_infos) + 1}", "url": url, "category": current_category})
                    else:
                        url_infos.append({"name": f"{current_category} Stream {len(url_infos) + 1}", "url": url, "category": current_category})

    def find_files_in_directory(self, directory, extensions=None, recursive=True):
        """
        Find all files with specified extensions in a directory.
        
        Args:
            directory (str): Directory to search in
            extensions (list): List of file extensions to include (e.g., ['.txt', '.m3u'])
            recursive (bool): Whether to search subdirectories recursively
            
        Returns:
            list: List of file paths
        """
        if extensions is None:
            extensions = ['.txt', '.m3u', '.m3u8', '.json']
            
        file_paths = []
        
        try:
            if recursive:
                # Walk through the directory and its subdirectories
                for root, _, files in os.walk(directory):
                    for file in files:
                        # Check if the file has one of the specified extensions
                        if any(file.lower().endswith(ext.lower()) for ext in extensions):
                            file_path = os.path.join(root, file)
                            file_paths.append(file_path)
            else:
                # Only search in the specified directory, not subdirectories
                for ext in extensions:
                    pattern = os.path.join(directory, f"*{ext}")
                    file_paths.extend(glob.glob(pattern))
                    # Also try with uppercase extension
                    pattern = os.path.join(directory, f"*{ext.upper()}")
                    file_paths.extend(glob.glob(pattern))
        except Exception as e:
            logger.error(f"Error searching directory {directory}: {str(e)}")
            
        return file_paths

    def save_checkpoint(self):
        """
        保存检查点，记录已处理的URL和当前结果
        """
        if not self.checkpoint_file:
            return
            
        checkpoint_data = {
            "processed_url_hashes": self.processed_url_hashes,
            "results": self.results,
            "total_urls": self.total_urls,
            "processed_urls": self.processed_urls
        }
        
        try:
            # 创建临时文件，成功后再重命名，避免写入过程中程序崩溃导致检查点文件损坏
            temp_file = f"{self.checkpoint_file}.tmp"
            with open(temp_file, 'wb') as f:
                pickle.dump(checkpoint_data, f)
            
            # 如果临时文件写入成功，则重命名为正式检查点文件
            if os.path.exists(temp_file):
                if os.path.exists(self.checkpoint_file):
                    os.remove(self.checkpoint_file)
                os.rename(temp_file, self.checkpoint_file)
                
            logger.info(f"Checkpoint saved: {self.processed_urls}/{self.total_urls} URLs processed")
        except Exception as e:
            logger.error(f"Error saving checkpoint: {str(e)}")

    def load_checkpoint(self):
        """
        加载检查点，恢复已处理的URL和结果
        
        Returns:
            bool: 是否成功加载检查点
        """
        if not self.checkpoint_file or not os.path.exists(self.checkpoint_file):
            return False
            
        try:
            with open(self.checkpoint_file, 'rb') as f:
                checkpoint_data = pickle.load(f)
                
            self.processed_url_hashes = checkpoint_data.get("processed_url_hashes", set())
            self.results = checkpoint_data.get("results", {"valid": [], "invalid": [], "timeout": [], "error": []})
            self.total_urls = checkpoint_data.get("total_urls", 0)
            self.processed_urls = checkpoint_data.get("processed_urls", 0)
            
            logger.info(f"Checkpoint loaded: {self.processed_urls}/{self.total_urls} URLs already processed")
            return True
        except Exception as e:
            logger.error(f"Error loading checkpoint: {str(e)}")
            return False

    def url_hash(self, url):
        """
        计算URL的哈希值，用于检查URL是否已处理
        
        Args:
            url (str): URL字符串
            
        Returns:
            str: URL的哈希值
        """
        import hashlib
        return hashlib.md5(url.encode()).hexdigest()

    async def save_intermediate_results(self):
        """
        保存中间结果到文件
        """
        if self.output_file:
            self.save_results(self.output_file)
            self.save_checkpoint()
            logger.info(f"Intermediate results saved after processing {self.processed_urls}/{self.total_urls} URLs")

    async def process_files(self, file_paths, output_file=None):
        """
        Process multiple files and check all URLs found.
        
        Args:
            file_paths (list): List of file paths to process
            output_file (str): Path to save results
            
        Returns:
            dict: Results of URL checks
        """
        self.output_file = output_file
        all_url_infos = []
        
        # Extract URLs from all files
        for file_path in file_paths:
            if os.path.isfile(file_path):
                file_url_infos = self.extract_urls_from_file(file_path)
                logger.info(f"Extracted {len(file_url_infos)} URLs from {file_path}")
                all_url_infos.extend(file_url_infos)
        
        # Remove duplicates while preserving order
        unique_url_infos = []
        seen_urls = set()
        for url_info in all_url_infos:
            url = url_info["url"]
            if url not in seen_urls:
                seen_urls.add(url)
                unique_url_infos.append(url_info)
        
        self.total_urls = len(unique_url_infos)
        logger.info(f"Found {self.total_urls} unique URLs to check")
        
        if self.total_urls == 0:
            logger.warning("No URLs found to check!")
            return self.results
        
        # 检查是否有检查点可以恢复
        if self.checkpoint_file and os.path.exists(self.checkpoint_file) and ENABLE_CHECKPOINT:
            checkpoint_loaded = self.load_checkpoint()
            if checkpoint_loaded:
                # 过滤掉已经处理过的URL
                filtered_url_infos = []
                for url_info in unique_url_infos:
                    url_hash = self.url_hash(url_info["url"])
                    if url_hash not in self.processed_url_hashes:
                        filtered_url_infos.append(url_info)
                
                logger.info(f"Skipping {self.total_urls - len(filtered_url_infos)} already processed URLs")
                unique_url_infos = filtered_url_infos
        
        # 如果所有URL都已处理，直接返回结果
        if not unique_url_infos:
            logger.info("All URLs have been processed in previous runs")
            if output_file:
                self.save_results(output_file)
            return self.results
            
        # Create progress bar
        self.progress_bar = tqdm(total=len(unique_url_infos), desc="Checking URLs", initial=self.processed_urls)
        
        # 分批处理URL，每批CONCURRENCY个
        for i in range(0, len(unique_url_infos), self.concurrency):
            batch = unique_url_infos[i:i+self.concurrency]
            
            # Check URLs in this batch concurrently
            tasks = [self.check_url(url_info) for url_info in batch]
            batch_results = await asyncio.gather(*tasks)
            
            # Process results
            for url_info, status, response_time, error_message in batch_results:
                # 记录已处理的URL
                url_hash = self.url_hash(url_info["url"])
                self.processed_url_hashes.add(url_hash)
                
                self.results[status].append({
                    "name": url_info.get("name", "Unknown"),
                    "url": url_info["url"],
                    "category": url_info.get("category", ""),
                    "response_time": round(response_time, 2),
                    "error": error_message
                })
            
            # 每批处理完成后保存中间结果
            await self.save_intermediate_results()
        
        # Close progress bar
        self.progress_bar.close()
        self.progress_bar = None
        
        # Save final results if output file is specified
        if output_file:
            self.save_results(output_file)
            
        # 处理完成后删除检查点文件
        if self.checkpoint_file and os.path.exists(self.checkpoint_file):
            try:
                os.remove(self.checkpoint_file)
                logger.info(f"Checkpoint file removed: {self.checkpoint_file}")
            except Exception as e:
                logger.error(f"Error removing checkpoint file: {str(e)}")
        
        return self.results
    
    def save_results(self, output_file):
        """
        Save results to a JSON file.
        
        Args:
            output_file (str): Path to save results
        """
        # Create directory if it doesn't exist
        output_dir = os.path.dirname(output_file)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)
            
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump({
                "summary": {
                    "total": self.total_urls,
                    "valid": len(self.results["valid"]),
                    "invalid": len(self.results["invalid"]),
                    "timeout": len(self.results["timeout"]),
                    "error": len(self.results["error"])
                },
                "results": self.results
            }, f, ensure_ascii=False, indent=2)
        
        logger.info(f"Results saved to {output_file}")
        
        # 生成中间文件
        valid_file = f"{os.path.splitext(output_file)[0]}_valid.txt"
        grouped_file = f"{os.path.splitext(output_file)[0]}_grouped.txt"
        final_file = f"{os.path.splitext(output_file)[0]}_final.txt"
        
        # 保存有效URL到文件
        self._save_valid_results(valid_file)
        logger.info(f"Valid URLs saved to {valid_file}")
        
        # 保存按名称相似度聚合的版本
        self._save_grouped_results(grouped_file)
        logger.info(f"Grouped valid URLs saved to {grouped_file}")
        
        # 创建最终排序的文件
        self._create_final_sorted_file(final_file)
        logger.info(f"Final sorted results saved to {final_file}")
        
        # 如果设置了只保留最终文件，则删除中间文件
        if KEEP_ONLY_FINAL_FILE:
            try:
                # 删除JSON结果文件
                if os.path.exists(output_file):
                    os.remove(output_file)
                    logger.info(f"Removed intermediate file: {output_file}")
                
                # 删除有效URL文件
                if os.path.exists(valid_file):
                    os.remove(valid_file)
                    logger.info(f"Removed intermediate file: {valid_file}")
                
                # 删除分组文件
                if os.path.exists(grouped_file):
                    os.remove(grouped_file)
                    logger.info(f"Removed intermediate file: {grouped_file}")
                
                logger.info("Cleanup completed, only final sorted file is kept")
            except Exception as e:
                logger.error(f"Error during cleanup: {str(e)}")
    
    def _save_valid_results(self, output_file):
        """
        保存有效URL到文件
        
        Args:
            output_file (str): 保存路径
        """
        # 按照类别和名称分组，并按响应时间排序
        categories = {}
        for item in self.results["valid"]:
            category = item.get("category", "未分类")
            if category not in categories:
                categories[category] = {}
                
            name = item.get("name", "Unknown")
            if name not in categories[category]:
                categories[category][name] = []
                
            categories[category][name].append(item)
        
        # 对每个名称下的链接按响应时间排序
        for category in categories.values():
            for name_items in category.values():
                name_items.sort(key=lambda x: x.get("response_time", float('inf')))
        
        with open(output_file, 'w', encoding='utf-8') as f:
            # 写入每个类别
            for category, names in categories.items():
                if category:
                    f.write(f"{category},#genre#\n")
                
                # 处理每个名称下的项目
                for name, items in names.items():
                    # 如果同一名称有多个链接，添加编号
                    if len(items) == 1:
                        item = items[0]
                        f.write(f"{name},{item['url']}\n")
                    else:
                        for i, item in enumerate(items):
                            f.write(f"{name} {i+1},{item['url']}\n")
                
                # 在类别之间添加空行
                if category:
                    f.write("\n")
    
    def _save_grouped_results(self, output_file):
        """
        保存按名称相似度聚合的结果
        
        Args:
            output_file (str): 保存路径
        """
        # 提取所有有效的项目
        valid_items = self.results["valid"]
        
        # 按名称的前几个字符分组
        name_groups = {}
        for item in valid_items:
            name = item.get("name", "Unknown")
            # 使用名称的前3个字符作为分组键（可以根据需要调整）
            if len(name) >= 3:
                key = name[:3]
            else:
                key = name
                
            if key not in name_groups:
                name_groups[key] = []
            name_groups[key].append(item)
        
        # 对每个组内的项目按响应时间排序
        for group in name_groups.values():
            group.sort(key=lambda x: x.get("response_time", float('inf')))
        
        # 按组名排序
        sorted_groups = sorted(name_groups.items())
        
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write("聚合分组,#genre#\n")
            
            # 为每个组写入项目
            for key, group in sorted_groups:
                # 使用计数器跟踪每个名称出现的次数
                name_counters = {}
                
                for item in group:
                    name = item.get("name", "Unknown")
                    url = item["url"]
                    
                    # 更新计数器并获取当前计数
                    if name in name_counters:
                        name_counters[name] += 1
                        count = name_counters[name]
                        f.write(f"{name} {count},{url}\n")
                    else:
                        name_counters[name] = 1
                        f.write(f"{name},{url}\n")
            
            f.write("\n")
            
            # 也按原始类别写入
            categories = {}
            for item in valid_items:
                category = item.get("category", "未分类")
                if category not in categories:
                    categories[category] = {}
                    
                name = item.get("name", "Unknown")
                if name not in categories[category]:
                    categories[category][name] = []
                    
                categories[category][name].append(item)
            
            # 对每个名称下的链接按响应时间排序
            for category in categories.values():
                for name_items in category.values():
                    name_items.sort(key=lambda x: x.get("response_time", float('inf')))
            
            # 写入每个类别
            for category, names in categories.items():
                if category and category != "未分类":
                    f.write(f"{category},#genre#\n")
                    
                    # 处理每个名称下的项目
                    for name, items in names.items():
                        # 如果同一名称有多个链接，添加编号
                        if len(items) == 1:
                            item = items[0]
                            f.write(f"{name},{item['url']}\n")
                        else:
                            for i, item in enumerate(items):
                                f.write(f"{name} {i+1},{item['url']}\n")
                    
                    f.write("\n")
    
    def _create_final_sorted_file(self, output_file):
        """
        创建最终排序的文件，按类别和名称排序
        
        Args:
            output_file (str): 保存路径
        """
        # 提取所有有效的项目
        valid_items = self.results["valid"]
        
        # 按类别和名称分组
        categories = {}
        for item in valid_items:
            category = item.get("category", "未分类")
            if category not in categories:
                categories[category] = {}
                
            name = item.get("name", "Unknown")
            if name not in categories[category]:
                categories[category][name] = []
                
            categories[category][name].append(item)
        
        # 对每个名称下的链接按响应时间排序
        for category in categories.values():
            for name_items in category.values():
                name_items.sort(key=lambda x: x.get("response_time", float('inf')))
        
        # 按类别名称排序
        sorted_categories = sorted(categories.items())
        
        with open(output_file, 'w', encoding='utf-8') as f:
            # 写入每个类别
            for category, names in sorted_categories:
                if category:
                    f.write(f"{category},#genre#\n")
                
                # 按名称排序
                sorted_names = sorted(names.items())
                
                # 处理每个名称下的项目
                for name, items in sorted_names:
                    # 如果同一名称有多个链接，添加编号
                    if len(items) == 1:
                        item = items[0]
                        f.write(f"{name},{item['url']}\n")
                    else:
                        for i, item in enumerate(items):
                            f.write(f"{name} {i+1},{item['url']}\n")
                
                # 在类别之间添加空行
                if category:
                    f.write("\n")
            
            # 只有当INCLUDE_AGGREGATED_SECTION为True时才添加聚合分组部分
            if INCLUDE_AGGREGATED_SECTION:
                # 添加聚合分组部分
                f.write("聚合分组,#genre#\n")
                
                # 按名称的前几个字符分组
                name_groups = {}
                for item in valid_items:
                    name = item.get("name", "Unknown")
                    # 使用名称的前3个字符作为分组键
                    if len(name) >= 3:
                        key = name[:3]
                    else:
                        key = name
                        
                    if key not in name_groups:
                        name_groups[key] = []
                    name_groups[key].append(item)
                
                # 对每个组内的项目按响应时间排序
                for group in name_groups.values():
                    group.sort(key=lambda x: x.get("response_time", float('inf')))
                
                # 按组名排序
                sorted_groups = sorted(name_groups.items())
                
                # 为每个组写入项目
                for key, group in sorted_groups:
                    # 使用计数器跟踪每个名称出现的次数
                    name_counters = {}
                    
                    for item in group:
                        name = item.get("name", "Unknown")
                        url = item["url"]
                        
                        # 更新计数器并获取当前计数
                        if name in name_counters:
                            name_counters[name] += 1
                            count = name_counters[name]
                            f.write(f"{name} {count},{url}\n")
                        else:
                            name_counters[name] = 1
                            f.write(f"{name},{url}\n")

    def _check_stream_content(self, content_bytes, url):
        """
        检查内容是否符合流媒体格式
        
        Args:
            content_bytes (bytes): 内容的前几个字节
            url (str): URL字符串
            
        Returns:
            bool: 是否可能是有效的流媒体
        """
        # 检查是否是m3u8格式
        if url.lower().endswith('.m3u8'):
            try:
                content = content_bytes.decode('utf-8')
                return content.startswith('#EXTM3U') or '#EXT-X-STREAM-INF' in content
            except:
                return False
        
        # 检查是否是TS文件
        if url.lower().endswith('.ts'):
            # TS文件通常以0x47开头
            return content_bytes.startswith(b'\x47')
        
        # 检查是否是MP4文件
        if url.lower().endswith('.mp4'):
            # MP4文件通常包含'ftyp'标记
            return b'ftyp' in content_bytes
        
        # 检查是否是FLV文件
        if url.lower().endswith('.flv'):
            # FLV文件以"FLV"开头
            return self._validate_flv(content_bytes)
        
        # 检查是否是PHP动态生成的流
        if '.php' in url.lower():
            # 检查是否包含流媒体格式的标记
            return (
                content_bytes.startswith(b'#EXTM3U') or  # m3u8
                content_bytes.startswith(b'\x47') or     # TS
                b'ftyp' in content_bytes or              # MP4
                self._validate_flv(content_bytes) or     # FLV
                b'<?xml' in content_bytes                # XML (可能是DASH或HLS清单)
            )
        
        # 如果没有特定的扩展名，尝试检测常见的流媒体格式
        return (
            content_bytes.startswith(b'#EXTM3U') or  # m3u8
            content_bytes.startswith(b'\x47') or     # TS
            b'ftyp' in content_bytes or              # MP4
            self._validate_flv(content_bytes) or     # FLV
            b'<?xml' in content_bytes                # XML (可能是DASH或HLS清单)
        )
    
    def _validate_flv(self, content_bytes):
        """
        验证FLV文件头
        
        Args:
            content_bytes (bytes): 内容的前几个字节
            
        Returns:
            bool: 是否是有效的FLV文件
        """
        # FLV文件头格式: 'FLV' + version(1) + flags(1) + headersize(4)
        if len(content_bytes) < 9:
            return False
        
        # 检查FLV签名
        if not content_bytes.startswith(b'FLV'):
            return False
        
        # 检查版本 (通常是1)
        version = content_bytes[3]
        if version != 1:
            return False
        
        # 检查标志位 (应该是1表示视频，4表示音频，5表示两者都有)
        flags = content_bytes[4]
        if flags not in [1, 4, 5]:
            return False
        
        # 检查头部大小 (通常是9)
        header_size = int.from_bytes(content_bytes[5:9], byteorder='big')
        if header_size != 9:
            return False
        
        return True
    
    async def _validate_m3u8(self, url, content, session, headers):
        """
        验证m3u8文件的有效性
        
        Args:
            url (str): m3u8文件的URL
            content (str): m3u8文件的内容
            session (aiohttp.ClientSession): HTTP会话
            headers (dict): HTTP请求头
            
        Returns:
            tuple: (是否有效, 错误信息)
        """
        # 检查内容是否符合m3u8格式
        if VALIDATE_M3U8_CONTENT and not content.startswith('#EXTM3U'):
            return False, "Not a valid M3U8 file (missing #EXTM3U header)"
        
        # 检查是否是主播放列表
        if '#EXT-X-STREAM-INF' in content:
            # 这是一个主播放列表，需要获取子播放列表
            variant_urls = []
            for line in content.splitlines():
                if line.startswith('http'):
                    variant_urls.append(line.strip())
                elif not line.startswith('#') and line.strip():
                    # 相对路径，需要与基础URL合并
                    base_url = url.rsplit('/', 1)[0] if '/' in url else url
                    variant_urls.append(urljoin(base_url + '/', line.strip()))
            
            if not variant_urls:
                return False, "No variant streams found in master playlist"
            
            # 检查第一个变体流
            try:
                async with session.get(variant_urls[0], headers=headers) as response:
                    if response.status >= 400:
                        return False, f"Variant stream HTTP status: {response.status}"
                    
                    variant_content = await response.text()
                    return await self._validate_media_playlist(variant_urls[0], variant_content, session, headers)
            except Exception as e:
                return False, f"Error checking variant stream: {str(e)}"
        else:
            # 这是一个媒体播放列表，直接验证
            return await self._validate_media_playlist(url, content, session, headers)
    
    async def _validate_media_playlist(self, url, content, session, headers):
        """
        验证媒体播放列表的有效性
        
        Args:
            url (str): 播放列表URL
            content (str): 播放列表内容
            session (aiohttp.ClientSession): HTTP会话
            headers (dict): HTTP请求头
            
        Returns:
            tuple: (是否有效, 错误信息)
        """
        # 提取媒体片段
        segments = []
        for line in content.splitlines():
            if not line.startswith('#') and line.strip():
                if line.startswith('http'):
                    segments.append(line.strip())
                else:
                    # 相对路径，需要与基础URL合并
                    base_url = url.rsplit('/', 1)[0] if '/' in url else url
                    segments.append(urljoin(base_url + '/', line.strip()))
        
        if not segments:
            return False, "No media segments found in playlist"
        
        # 检查前几个片段是否可访问
        segments_to_check = min(len(segments), M3U8_SEGMENTS_TO_CHECK)
        for i in range(segments_to_check):
            try:
                async with session.head(segments[i], headers=headers) as response:
                    if response.status >= 400:
                        return False, f"Segment {i+1} HTTP status: {response.status}"
            except Exception as e:
                return False, f"Error checking segment {i+1}: {str(e)}"
        
        return True, None
    
    async def _validate_with_ffprobe(self, url):
        """
        使用ffprobe验证流媒体
        
        Args:
            url (str): 流媒体URL
            
        Returns:
            tuple: (是否有效, 错误信息)
        """
        try:
            import subprocess
            
            # 构建ffprobe命令
            cmd = [
                'ffprobe',
                '-v', 'error',
                '-show_entries', 'stream=codec_type',
                '-of', 'json',
                '-timeout', str(self.timeout * 1000000),  # 微秒
                url
            ]
            
            # 执行命令
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            
            # 设置超时
            try:
                stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=self.timeout)
            except asyncio.TimeoutError:
                process.kill()
                return False, "ffprobe timeout"
            
            # 检查是否有错误
            if process.returncode != 0:
                return False, f"ffprobe error: {stderr.decode('utf-8', errors='ignore')}"
            
            # 解析输出
            try:
                result = json.loads(stdout.decode('utf-8', errors='ignore'))
                streams = result.get('streams', [])
                
                # 检查是否有视频或音频流
                if not streams:
                    return False, "No streams found"
                
                # 检查流类型
                stream_types = [stream.get('codec_type') for stream in streams]
                if 'video' in stream_types or 'audio' in stream_types:
                    return True, None
                else:
                    return False, "No video or audio streams found"
            except json.JSONDecodeError:
                return False, "Failed to parse ffprobe output"
        except Exception as e:
            return False, f"Error using ffprobe: {str(e)}"

async def check_directory(directory, extensions=None, concurrency=100, timeout=10, 
                         output_file="results.json", recursive=True, user_agent=None,
                         save_interval=100, checkpoint_file=None):
    """
    Convenience function to check all files in a directory.
    
    Args:
        directory (str): Directory to scan
        extensions (list): File extensions to include
        concurrency (int): Maximum number of concurrent requests
        timeout (int): Timeout for each request in seconds
        output_file (str): Path to save results
        recursive (bool): Whether to search subdirectories recursively
        user_agent (str): User agent string for HTTP requests
        save_interval (int): Save results after processing this many URLs
        checkpoint_file (str): Path to save checkpoint data
        
    Returns:
        dict: Results of URL checks
    """
    # Create checker
    checker = StreamChecker(
        concurrency=concurrency,
        timeout=timeout,
        user_agent=user_agent,
        save_interval=save_interval,
        checkpoint_file=checkpoint_file
    )
    
    # Find files
    if os.path.isdir(directory):
        logger.info(f"Scanning directory: {directory}")
        file_paths = checker.find_files_in_directory(directory, extensions, recursive)
        logger.info(f"Found {len(file_paths)} files with extensions: {', '.join(extensions or ['.txt', '.m3u', '.m3u8', '.json'])}")
    else:
        logger.error(f"Directory not found: {directory}")
        return None
    
    if not file_paths:
        logger.error("No files found to process!")
        return None
        
    # Process files
    start_time = time.time()
    results = await checker.process_files(file_paths, output_file)
    elapsed_time = time.time() - start_time
    
    # Print summary
    print("\nSummary:")
    print(f"Total URLs: {checker.total_urls}")
    if checker.total_urls > 0:
        print(f"Valid: {len(results['valid'])} ({len(results['valid'])/checker.total_urls*100:.1f}%)")
        print(f"Invalid: {len(results['invalid'])} ({len(results['invalid'])/checker.total_urls*100:.1f}%)")
        print(f"Timeout: {len(results['timeout'])} ({len(results['timeout'])/checker.total_urls*100:.1f}%)")
        print(f"Error: {len(results['error'])} ({len(results['error'])/checker.total_urls*100:.1f}%)")
    print(f"Total time: {elapsed_time:.2f} seconds")
    
    if output_file:
        print(f"Detailed results saved to {output_file}")
        valid_file = f"{os.path.splitext(output_file)[0]}_valid.txt"
        print(f"Valid URLs with names saved to {valid_file}")
        
    return results

# 主程序入口
if __name__ == "__main__":
    print("直播源链接检测工具")
    print("=" * 50)
    print(f"使用配置:")
    print(f"- 扫描目录: {DIRECTORY}")
    print(f"- 文件扩展名: {', '.join(EXTENSIONS)}")
    print(f"- 并发数: {CONCURRENCY}")
    print(f"- 超时时间: {TIMEOUT} 秒")
    print(f"- 递归扫描: {'是' if RECURSIVE else '否'}")
    print(f"- 输出文件: {OUTPUT_FILE}")
    print(f"- 断点续传: {'启用' if ENABLE_CHECKPOINT else '禁用'}")
    print(f"- 保存间隔: 每处理 {SAVE_INTERVAL} 个URL")
    print(f"- 繁体转简体: {'启用' if CONVERT_TO_SIMPLIFIED and HAS_OPENCC else '禁用'}")
    print(f"- 只保留最终文件: {'是' if KEEP_ONLY_FINAL_FILE else '否'}")
    print(f"- 在最终文件中包含聚合分组部分: {'是' if INCLUDE_AGGREGATED_SECTION else '否'}")
    print(f"- 深度验证m3u8: {'是' if DEEP_VALIDATION else '否'}")
    print(f"- 验证其他格式的流媒体: {'是' if VALIDATE_OTHER_STREAMS else '否'}")
    print(f"- 使用ffprobe进行高级验证: {'是' if USE_FFPROBE else '否'}")
    print("=" * 50)
    
    asyncio.run(check_directory(
        directory=DIRECTORY,
        extensions=EXTENSIONS,
        concurrency=CONCURRENCY,
        timeout=TIMEOUT,
        output_file=OUTPUT_FILE,
        recursive=RECURSIVE,
        user_agent=USER_AGENT,
        save_interval=SAVE_INTERVAL,
        checkpoint_file=CHECKPOINT_FILE if ENABLE_CHECKPOINT else None
    )) 