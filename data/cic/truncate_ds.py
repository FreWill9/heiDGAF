def copy_lines(input_path, output_path, start_line, end_line):
    """
    Copies lines from start_line to end_line (inclusive) from input_path to output_path.
    """
    if start_line < 1 or end_line < start_line:
        raise ValueError("Invalid start_line or end_line values")

    with open(input_path, 'r', encoding='utf-8') as infile, \
            open(output_path, 'w', encoding='utf-8') as outfile:

        for current_line_num, line in enumerate(infile, start=1):
            if current_line_num > end_line:
                break
            if current_line_num >= start_line:
                outfile.write(line)


def main():
    input_path = "../../zeek_input_dir/shortened_dns.log"
    output_path = "../../zeek_input_dir/shortened_to_bug_dns.log"
    start_line = 400_000
    end_line = 623_272
    copy_lines(input_path, output_path, start_line, end_line)


if __name__ == '__main__':
    main()
