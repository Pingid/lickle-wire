import { defineConfig, Place, Select, Match } from '@lickle/docs/config'

export default defineConfig({
  name: '@lickle/wire',
  layout: Place.compose(
    Place.filter(Match.all(Match.exposed(), Match.not(Match.tag('@internal')))),
    Place.bucket(Select.kind),
    Place.bucket(Match.kinds('interface', 'type-alias', 'namespace'), 'types'),
    Place.visibility(Match.bucket('types'), { nav: false, page: true, inline: false }),
    Place.bucketOrder('modules', /.*/),
  ),
})
