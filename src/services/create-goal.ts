import { db } from '../db'
import { goals } from '../db/schema'

type createGoalResquest = {
  title: string
  desiredWeeklyFrequency: number
}

export async function createGoal({
  title,
  desiredWeeklyFrequency,
}: createGoalResquest) {
  const result = await db
    .insert(goals)
    .values({ title, desiredWeeklyFrequency })
    .returning()

  const goal = result[0]

  return {
    goal,
  }
}
