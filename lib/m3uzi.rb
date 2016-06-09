$:<< File.dirname(__FILE__)
require 'm3uzi/item'
require 'm3uzi/tag'
require 'm3uzi/file'
require 'm3uzi/stream'
require 'm3uzi/comment'
require 'm3uzi/version'
require 'tempfile'

class M3Uzi

  attr_accessor :header_tags, :playlist_items
  attr_accessor :playlist_type, :final_media_file
  attr_accessor :version, :initial_media_sequence, :sliding_window_duration
  attr_accessor :path, :crlf

  def initialize
    @header_tags = []
    @playlist_items = []
    @final_media_file = true
    @version = 1
    @initial_media_sequence = 0
    @sliding_window_duration = nil
    @removed_file_count = 0
    @playlist_type = :live
    @crlf = "\r\n"
  end


  #-------------------------------------
  # Read/Write M3U8 Files
  #-------------------------------------

  ##
  ## For now, reading m3u8 files is not keeping up to date with writing, so we're
  ## disabling it in this version.  (Possibly to be re-introduced in the future.)
  ##
  def self.read(path, encoding='iso-8859-1')
    m3u = self.new
    m3u.path = path
    lines = ::File.readlines(path, encoding: encoding)
    lines.each_with_index do |line, i|
      line.strip!
      case type(line)
      when :tag
        name, value = parse_general_tag(line)
        next if name == 'M3U'
        m3u.add_tag do |tag|
          tag.source_line = i+1
          tag.name = name
          tag.value = value
        end
      when :info
        duration, description = parse_file_tag(line)
        m3u.add_file do |file|
          file.path = lines[i+1].strip
          file.source_line = i+1
          file.duration = duration
          file.description = description
        end
        m3u.final_media_file = false
      when :stream
        attributes = parse_stream_tag(line)
        m3u.add_stream do |stream|
          stream.source_line = i+1
          stream.path = lines[i+1].strip
          attributes.each_pair do |k,v|
            k = k.to_s.downcase.sub('-','_')
            next unless [:bandwidth, :program_id, :codecs, :resolution].include?(k)
            v = $1 if v.to_s =~ /^"(.*)"$/
            stream.send("#{k}=", v)
          end
        end
      when :final
        m3u.final_media_file = true
      else
        next
      end
    end
    m3u
  end
  
  def self.clean(path, encoding='iso-8859-1')
    m3u = read(path, encoding)
    file_count = m3u.items(File).size
    valid_files = m3u.valid_items(File)
    if valid_files.size < file_count
      dir = ::File.dirname(path)
      basename = ::File.basename(path, '.*')
      ext = ::File.extname(path)
      tempfile = ::Tempfile.new([basename, ext], encoding: encoding)
      m3u.version = 0 # winamp
      m3u.write_to_io(tempfile)
      tempfile
    else
      nil
    end
  end

  def write_to_io(io_stream)
    reset_encryption_key_history
    reset_byterange_history

    check_version_restrictions
    io_stream << ("#EXTM3U" + @crlf)
    io_stream << ("#EXT-X-VERSION:#{@version.to_i}" + @crlf) if @version > 1
    io_stream << ("#EXT-X-PLAYLIST-TYPE:#{@playlist_type.to_s.upcase}" + @crlf) if [:event,:vod].include?(@playlist_type)

    if @version > 0 && items(File).length > 0
      io_stream << ("#EXT-X-MEDIA-SEQUENCE:#{@initial_media_sequence+@removed_file_count}" + @crlf) if @playlist_type == :live
      max_duration = valid_items(File).map { |f| f.duration.to_f }.max || 10.0
      io_stream << ("#EXT-X-TARGETDURATION:#{max_duration.ceil}"  + @crlf)
    end

    @header_tags.each do |item|
      io_stream << (item.format + @crlf) if item.valid?
    end

    @playlist_items.each do |item|
      next unless item.valid?

      if item.kind_of?(File)
        encryption_key_line = generate_encryption_key_line(item)
        io_stream << (encryption_key_line + @crlf) if encryption_key_line

        byterange_line = generate_byterange_line(item)
        io_stream << (byterange_line + @crlf) if byterange_line
      end

      io_stream << (item.format + @crlf)
    end

    io_stream << ("#EXT-X-ENDLIST" + @crlf) if items(File).length > 0 && (@final_media_file || @playlist_type == :vod)
  end

  def write(path, encoding='iso-8859-1')
    ::File.open(path, 'w', encoding: encoding) { |f| write_to_io(f) }
  end

  def items(kind)
    @playlist_items.select { |item| item.kind_of?(kind) }
  end

  def valid_items(kind)
    @playlist_items.select { |item| item.kind_of?(kind) && item.valid? }
  end

  def invalid_items(kind)
    @playlist_items.select { |item| item.kind_of?(kind) && !item.valid? }
  end

  #-------------------------------------
  # Playlist generation helpers.
  #-------------------------------------

  def reset_encryption_key_history
    @encryption_key_url = nil
    @encryption_iv = nil
    @encryption_sequence = 0
  end

  def generate_encryption_key_line(file)
    generate_line = false

    default_iv = @encryption_iv || format_iv(@encryption_sequence)

    if (file.encryption_key_url != :unset) && (file.encryption_key_url != @encryption_key_url)
      @encryption_key_url = file.encryption_key_url
      generate_line = true
    end

    if @encryption_key_url && file.encryption_iv != @encryption_iv
      @encryption_iv = file.encryption_iv
      generate_line = true
    end

    @encryption_sequence += 1

    if generate_line
      if @encryption_key_url.nil?
        "#EXT-X-KEY:METHOD=NONE"
      else
        attrs = ['METHOD=AES-128']
        attrs << 'URI="' + @encryption_key_url.gsub('"','%22').gsub(/[\r\n]/,'').strip + '"'
        attrs << "IV=#{@encryption_iv}" if @encryption_iv
        '#EXT-X-KEY:' + attrs.join(',')
      end
    else
      nil
    end
  end

  def reset_byterange_history
    @prev_byterange_endpoint = nil
  end

  def generate_byterange_line(file)
    line = nil

    if file.byterange
      if file.byterange_offset && file.byterange_offset != @prev_byterange_endpoint
        offset = file.byterange_offset
      elsif @prev_byterange_endpoint.nil?
        offset = 0
      else
        offset = nil
      end

      line = "#EXT-X-BYTERANGE:#{file.byterange_offset.to_i}"
      line += "@#{offset}" if offset

      @prev_byterange_endpoint = offset + file.byterange
    else
      @prev_byterange_endpoint = nil
    end

    line
  end


  #-------------------------------------
  # Files
  #-------------------------------------

  def add_file(path = nil, duration = nil)
    new_file = M3Uzi::File.new
    new_file.path = path if path
    new_file.duration = duration if duration
    yield(new_file) if block_given?
    @playlist_items << new_file
    cleanup_sliding_window
  end

  def filenames
    items(File).map { |file| file.path }
  end


  #-------------------------------------
  # Streams
  #-------------------------------------

  def add_stream(path = nil, bandwidth = nil)
    new_stream = M3Uzi::Stream.new
    new_stream.path = path
    new_stream.bandwidth = bandwidth
    yield(new_stream) if block_given?
    @playlist_items << new_stream
  end

  def stream_names
    items(Stream).map { |stream| stream.path }
  end


  #-------------------------------------
  # Tags
  #-------------------------------------

  def add_tag(name = nil, value = nil)
    new_tag = M3Uzi::Tag.new
    new_tag.name = name
    new_tag.value = value
    yield(new_tag) if block_given?
    @header_tags << new_tag
  end

  # def [](key)
  #   tag_name = key.to_s.upcase.gsub("_", "-")
  #   obj = tags.detect { |tag| tag.name == tag_name }
  #   obj && obj.value
  # end
  #
  # def []=(key, value)
  #   add_tag do |tag|
  #     tag.name = key
  #     tag.value = value
  #   end
  # end


  #-------------------------------------
  # Comments
  #-------------------------------------

  def add_comment(comment = nil)
    new_comment = M3Uzi::Comment.new
    new_comment.text = comment
    yield(new_comment) if block_given?
    @playlist_items << new_comment
  end

  # def <<(comment)
  #   add_comment(comment)
  # end

  def check_version_restrictions
    #
    # Version 2 Features
    #

    # Check for custom IV
    if valid_items(File).detect { |item| item.encryption_key_url && item.encryption_iv }
      @version = 2 if @version < 2
    end

    # Version 3 Features
    if valid_items(File).detect { |item| item.duration.kind_of?(Float) }
      @version = 3 if @version < 3
    end

    # Version 4 Features
    if valid_items(File).detect { |item| item.byterange }
      @version = 4 if @version < 4
    end
    if valid_items(Tag).detect { |item| ['MEDIA','I-FRAMES-ONLY'].include?(item.name) }
      @version = 4 if @version < 4
    end

    # NOTES
    #   EXT-X-I-FRAME-STREAM-INF is supposed to be ignored by older clients.
    #   AUDIO/VIDEO attributes of X-STREAM-INF are used in conjunction with MEDIA, so it should trigger v4.

    @version
  end

protected

  def self.type(line)
    case line
    when /^\s*$/
     :whitespace
    when /^#(?!EXT)/
     :comment
    when /^#EXTINF/
     :info
    when /^#EXT(-X)?-STREAM-INF/
     :stream
    when /^#EXT(-X)?-ENDLIST/
     :final
    when /^#EXT(?!INF)/
     :tag
    else
     :file
    end
  end

  def self.parse_general_tag(line)
    line.match(/^#EXT(?:-X-)?(?!STREAM-INF)([^:\n]+)(:([^\n]+))?$/).values_at(1, 3)
  end

  def self.parse_file_tag(line)
    line.match(/^#EXTINF:[ \t]*([-\d]+),?[ \t]*(.*)$/).values_at(1, 2)
  end

  def self.parse_stream_tag(line)
    match = line.match(/^#EXT-X-STREAM-INF:(.*)$/)[1]
    match.scan(/([A-Z-]+)\s*=\s*("[^"]*"|[^,]*)/) # return attributes as array of arrays
  end

  def cleanup_sliding_window
    return unless @sliding_window_duration && @playlist_type == :live
    while total_duration > @sliding_window_duration
      first_file = @playlist_items.detect { |item| item.kind_of?(File) && item.valid? }
      @playlist_items.delete(first_file)
      @removed_file_count += 1
    end
  end

  def total_duration
    valid_items(File).inject(0.0) { |d,f| d + f.duration.to_f }
  end

  def self.format_iv(num)
    '0x' + num.to_s(16).rjust(32,'0')
  end

  def format_iv(num)
    self.class.format_iv(num)
  end
end
