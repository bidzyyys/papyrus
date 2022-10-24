use crate::BlockNumber;

#[test]
fn block_number_iteration() {
    let start: u64 = 3;
    let up_until: u64 = 10;

    let mut expected = vec![];
    for i in start..up_until {
        expected.push(BlockNumber::new(i));
    }

    let start_block_number = BlockNumber::new(start);
    let up_until_block_number = BlockNumber::new(up_until);

    let mut from_iter: Vec<_> = vec![];
    for i in start_block_number.iter_up_to(up_until_block_number) {
        from_iter.push(i);
    }

    assert_eq!(expected, from_iter);
}
